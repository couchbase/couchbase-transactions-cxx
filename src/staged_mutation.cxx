#include <utility>

#include <utility>
#include <json11.hpp>

#include <libcouchbase/transactions/staged_mutation.hxx>
#include <libcouchbase/transactions/transaction_fields.hxx>

couchbase::transactions::staged_mutation::staged_mutation(couchbase::transactions::transaction_document &doc, json11::Json content,
                                                          couchbase::transactions::staged_mutation_type type)
    : doc_(std::move(doc)), content_(std::move(content)), type_(type)
{
}

const couchbase::transactions::transaction_document &couchbase::transactions::staged_mutation::doc() const
{
    return doc_;
}

const couchbase::transactions::staged_mutation_type &couchbase::transactions::staged_mutation::type() const
{
    return type_;
}

const json11::Json &couchbase::transactions::staged_mutation::content() const
{
    return content_;
}

bool couchbase::transactions::staged_mutation_queue::empty()
{
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.empty();
}

void couchbase::transactions::staged_mutation_queue::add(const couchbase::transactions::staged_mutation &mutation)
{
    std::unique_lock<std::mutex> lock(mutex_);
    queue_.push_back(mutation);
}
void couchbase::transactions::staged_mutation_queue::extract_to(const std::string &prefix, std::vector<couchbase::mutate_in_spec> &specs)
{
    std::unique_lock<std::mutex> lock(mutex_);
    json11::Json::array inserts;
    json11::Json::array replaces;
    json11::Json::array removes;

    for (auto &mutation : queue_) {
        json11::Json doc = json11::Json::object{
            { ATR_FIELD_PER_DOC_ID, mutation.doc().id() },
            { ATR_FIELD_PER_DOC_CAS, std::to_string(mutation.doc().cas()) },
            { ATR_FIELD_PER_DOC_BUCKET, mutation.doc().collection_ref().bucket_name() },
            { ATR_FIELD_PER_DOC_SCOPE, mutation.doc().collection_ref().scope() },
            { ATR_FIELD_PER_DOC_COLLECTION, mutation.doc().collection_ref().name() },
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
    specs.push_back(mutate_in_spec::upsert(prefix + ATR_FIELD_DOCS_INSERTED, inserts).xattr());
    specs.push_back(mutate_in_spec::upsert(prefix + ATR_FIELD_DOCS_REPLACED, replaces).xattr());
    specs.push_back(mutate_in_spec::upsert(prefix + ATR_FIELD_DOCS_REMOVED, removes).xattr());
}

couchbase::transactions::staged_mutation *couchbase::transactions::staged_mutation_queue::find_replace(couchbase::collection *collection,
                                                                                                       const std::string &id)
{
    for (auto &item : queue_) {
        if (item.type() == staged_mutation_type::REPLACE && item.doc().id() == id &&
            item.doc().collection_ref().bucket_name() == collection->bucket_name() &&
            item.doc().collection_ref().scope() == collection->scope() && item.doc().collection_ref().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}

couchbase::transactions::staged_mutation *couchbase::transactions::staged_mutation_queue::find_insert(couchbase::collection *collection,
                                                                                                      const std::string &id)
{
    for (auto &item : queue_) {
        if (item.type() == staged_mutation_type::INSERT && item.doc().id() == id &&
            item.doc().collection_ref().bucket_name() == collection->bucket_name() &&
            item.doc().collection_ref().scope() == collection->scope() && item.doc().collection_ref().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}

couchbase::transactions::staged_mutation *couchbase::transactions::staged_mutation_queue::find_remove(couchbase::collection *collection,
                                                                                                      const std::string &id)
{
    for (auto &item : queue_) {
        if (item.type() == staged_mutation_type::REMOVE && item.doc().id() == id &&
            item.doc().collection_ref().bucket_name() == collection->bucket_name() &&
            item.doc().collection_ref().scope() == collection->scope() && item.doc().collection_ref().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}
