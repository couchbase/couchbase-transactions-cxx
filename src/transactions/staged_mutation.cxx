#include <utility>
#include <spdlog/spdlog.h>
#include <couchbase/transactions/staged_mutation.hxx>
#include <couchbase/transactions/transaction_fields.hxx>

namespace tx = couchbase::transactions;

bool
tx::staged_mutation_queue::empty()
{
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.empty();
}

void
tx::staged_mutation_queue::add(const tx::staged_mutation& mutation)
{
    std::unique_lock<std::mutex> lock(mutex_);
    queue_.push_back(mutation);
}

void
tx::staged_mutation_queue::extract_to(const std::string& prefix, std::vector<couchbase::mutate_in_spec>& specs)
{
    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<nlohmann::json> inserts;
    std::vector<nlohmann::json> replaces;
    std::vector<nlohmann::json> removes;

    for (auto& mutation : queue_) {
        nlohmann::json doc{ { ATR_FIELD_PER_DOC_ID, mutation.doc().id() },
                            { ATR_FIELD_PER_DOC_BUCKET, mutation.doc().collection_ref().bucket_name() },
                            { ATR_FIELD_PER_DOC_SCOPE, mutation.doc().collection_ref().scope() },
                            { ATR_FIELD_PER_DOC_COLLECTION, mutation.doc().collection_ref().name() } };
        switch (mutation.type()) {
            case staged_mutation_type::INSERT:
                inserts.push_back(doc);
                break;
            case staged_mutation_type::REMOVE:
                removes.push_back(doc);
                break;
            case staged_mutation_type::REPLACE:
                replaces.push_back(doc);
                break;
        }
    }
    specs.push_back(mutate_in_spec::upsert(prefix + ATR_FIELD_DOCS_INSERTED, inserts).xattr());
    specs.push_back(mutate_in_spec::upsert(prefix + ATR_FIELD_DOCS_REPLACED, replaces).xattr());
    specs.push_back(mutate_in_spec::upsert(prefix + ATR_FIELD_DOCS_REMOVED, removes).xattr());
}

tx::staged_mutation*
tx::staged_mutation_queue::find_replace(std::shared_ptr<couchbase::collection> collection, const std::string& id)
{
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (item.type() == staged_mutation_type::REPLACE && item.doc().id() == id &&
            item.doc().collection_ref().bucket_name() == collection->bucket_name() &&
            item.doc().collection_ref().scope() == collection->scope() && item.doc().collection_ref().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}

tx::staged_mutation*
tx::staged_mutation_queue::find_insert(std::shared_ptr<couchbase::collection> collection, const std::string& id)
{
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (item.type() == staged_mutation_type::INSERT && item.doc().id() == id &&
            item.doc().collection_ref().bucket_name() == collection->bucket_name() &&
            item.doc().collection_ref().scope() == collection->scope() && item.doc().collection_ref().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}

tx::staged_mutation*
tx::staged_mutation_queue::find_remove(std::shared_ptr<couchbase::collection> collection, const std::string& id)
{
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (item.type() == staged_mutation_type::REMOVE && item.doc().id() == id &&
            item.doc().collection_ref().bucket_name() == collection->bucket_name() &&
            item.doc().collection_ref().scope() == collection->scope() && item.doc().collection_ref().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}

void
tx::staged_mutation_queue::commit()
{
    std::unique_lock<std::mutex> lock(mutex_);

    for (auto& item : queue_) {
        switch (item.type()) {
            case staged_mutation_type::REMOVE:
                item.doc().collection_ref().remove(item.doc().id());
                break;
            case staged_mutation_type::INSERT:
            case staged_mutation_type::REPLACE:
                // TODO: check and handle expiry, check for overtime mode, etc...

                // move staged content into doc
                item.doc().collection_ref().mutate_in(item.doc().id(),
                                                      {
                                                        mutate_in_spec::upsert(TRANSACTION_INTERFACE_PREFIX_ONLY, nullptr).xattr(),
                                                        mutate_in_spec::fulldoc_upsert(item.content<nlohmann::json>()),
                                                      });
                break;
        }
    }
}
