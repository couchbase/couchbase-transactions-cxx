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
        int vbucket_id = atr_ids::vbucket_for_key(id);
        atr_id_.emplace(atr_ids::atr_id_for_vbucket(vbucket_id));
        atr_collection_ = collection;
        state_ = attempt_state::PENDING;
        spdlog::info("first mutated doc in transaction is \"{}\" on vbucket {}, so using atr \"{}\"", id, vbucket_id, atr_id_.value());
    }
}

void
couchbase::transactions::attempt_context::check_atr_entry_for_blocking_document(const couchbase::transactions::transaction_document& doc)
{
    // FIXME:
    // collection = parent_->cleanup().cluster().bucket(doc.links().atr_bucket_name().value());
}
