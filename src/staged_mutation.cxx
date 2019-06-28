#include <utility>
#include <json11.hpp>

#include <libcouchbase/transactions/staged_mutation.hxx>
#include <libcouchbase/transactions/transaction_fields.hxx>

couchbase::transactions::StagedMutation::StagedMutation(couchbase::transactions::TransactionDocument &doc, const std::string &content,
                                                        couchbase::transactions::StagedMutationType type)
    : doc_(std::move(doc)), content_(std::move(content)), type_(type)
{
}

const couchbase::transactions::TransactionDocument &couchbase::transactions::StagedMutation::doc() const
{
    return doc_;
}

const couchbase::transactions::StagedMutationType &couchbase::transactions::StagedMutation::type() const
{
    return type_;
}

const std::string &couchbase::transactions::StagedMutation::content() const
{
    return content_;
}

bool couchbase::transactions::StagedMutationQueue::empty()
{
    std::unique_lock< std::mutex > lock(mutex_);
    return queue_.empty();
}

void couchbase::transactions::StagedMutationQueue::add(const couchbase::transactions::StagedMutation &mutation)
{
    std::unique_lock< std::mutex > lock(mutex_);
    queue_.push_back(mutation);
}
void couchbase::transactions::StagedMutationQueue::extract_to(const std::string &prefix, std::vector< couchbase::MutateInSpec > &specs)
{
    std::unique_lock< std::mutex > lock(mutex_);
    json11::Json::array inserts;
    json11::Json::array replaces;
    json11::Json::array removes;

    for (auto &mutation : queue_) {
        json11::Json doc = json11::Json::object{
            { ATR_FIELD_PER_DOC_ID, mutation.doc().id() },
            { ATR_FIELD_PER_DOC_CAS, std::to_string(mutation.doc().cas()) },
            { ATR_FIELD_PER_DOC_BUCKET, mutation.doc().collection().bucket_name() },
            { ATR_FIELD_PER_DOC_SCOPE, mutation.doc().collection().scope() },
            { ATR_FIELD_PER_DOC_COLLECTION, mutation.doc().collection().name() },
        };
        switch (mutation.type()) {
            case INSERT:
                inserts.push_back(doc);
                break;
            case REMOVE:
                removes.push_back(doc);
                break;
            case REPLACE:
                replaces.push_back(doc);
                break;
        }
    }
    specs.push_back(MutateInSpec::upsert(prefix + ATR_FIELD_DOCS_INSERTED, json11::Json(inserts).dump()).xattr());
    specs.push_back(MutateInSpec::upsert(prefix + ATR_FIELD_DOCS_REPLACED, json11::Json(replaces).dump()).xattr());
    specs.push_back(MutateInSpec::upsert(prefix + ATR_FIELD_DOCS_REMOVED, json11::Json(removes).dump()).xattr());
}

couchbase::transactions::StagedMutation *couchbase::transactions::StagedMutationQueue::find_replace(couchbase::Collection *collection,
                                                                                                    const std::string &id)
{
    for (auto &item : queue_) {
        if (item.type() == StagedMutationType::REPLACE && item.doc().id() == id &&
            item.doc().collection().bucket_name() == collection->bucket_name() && item.doc().collection().scope() == collection->scope() &&
            item.doc().collection().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}

couchbase::transactions::StagedMutation *couchbase::transactions::StagedMutationQueue::find_insert(couchbase::Collection *collection,
                                                                                                   const std::string &id)
{
    for (auto &item : queue_) {
        if (item.type() == StagedMutationType::INSERT && item.doc().id() == id &&
            item.doc().collection().bucket_name() == collection->bucket_name() && item.doc().collection().scope() == collection->scope() &&
            item.doc().collection().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}

couchbase::transactions::StagedMutation *couchbase::transactions::StagedMutationQueue::find_remove(couchbase::Collection *collection,
                                                                                                   const std::string &id)
{
    for (auto &item : queue_) {
        if (item.type() == StagedMutationType::REMOVE && item.doc().id() == id &&
            item.doc().collection().bucket_name() == collection->bucket_name() && item.doc().collection().scope() == collection->scope() &&
            item.doc().collection().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}
