#include <couchbase/client/collection.hxx>

#include <couchbase/transactions.hxx>
#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/attempt_state.hxx>
#include <couchbase/transactions/logging.hxx>

#include "atr_ids.hxx"

namespace tx = couchbase::transactions;

void
tx::attempt_context::select_atr_if_needed(std::shared_ptr<couchbase::collection> collection, const std::string& id)
{
    if (!atr_id_) {
        int vbucket_id=-1;
        boost::optional<const std::string> hook_atr = hooks_.random_atr_id_for_vbucket(this);
        if (hook_atr) {
            atr_id_.emplace(*hook_atr);
        } else {
            vbucket_id = atr_ids::vbucket_for_key(id);
            atr_id_.emplace(atr_ids::atr_id_for_vbucket(vbucket_id));
        }
        atr_collection_ = collection;
        overall_.atr_collection(collection->name());
        overall_.atr_id(*atr_id_);
        attempt_state(couchbase::transactions::attempt_state::NOT_STARTED);
        spdlog::info("first mutated doc in transaction is \"{}\" on vbucket {}, so using atr \"{}\"", id, vbucket_id, atr_id_.value());
    }
}

void
couchbase::transactions::attempt_context::check_atr_entry_for_blocking_document(const couchbase::transactions::transaction_document& doc)
{
    auto collection = parent_->cluster().bucket(doc.links().atr_bucket_name().value())->default_collection();
    int retries = 0;
    while (retries < 5) {
        retries++;
        auto atr = active_transaction_record::get_atr(collection, doc.links().atr_id().value(), config_);
        if (atr) {
            auto entries = atr->entries();
            auto it = std::find_if(entries.begin(), entries.end(), [&] (const atr_entry& e) {
                    return e.attempt_id() == doc.links().staged_attempt_id();
                    });
            if (it != entries.end()) {
                if (it->has_expired()) {
                    spdlog::trace("existing atr entry has expired, ignoring");
                    return;
                }
                switch(it->state()) {
                    case tx::attempt_state::COMPLETED:
                    case tx::attempt_state::ROLLED_BACK:
                        spdlog::trace("existing atr entry can be ignored due to state {}", it->state());
                        return;
                    default:
                        spdlog::trace("existing atr entry found in state {}, retrying in 100ms", it->state());
                }
                // TODO  this (and other retries) probably need a clever class, exponential backoff, etc...
                std::this_thread::sleep_for(std::chrono::milliseconds(50*retries));
            } else {
                spdlog::trace("no blocking atr entry");
                return;
            }
        }
    }
    // if we are here, there is still a write-write conflict
    throw error_wrapper(FAIL_WRITE_WRITE_CONFLICT, "document is in another transaction", true, true);
}
