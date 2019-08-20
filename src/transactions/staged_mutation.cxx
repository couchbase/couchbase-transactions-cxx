#include <utility>

#include <couchbase/transactions/staged_mutation.hxx>
#include <couchbase/transactions/transaction_fields.hxx>

namespace tx = couchbase::transactions;

tx::staged_mutation::staged_mutation(tx::transaction_document &doc, folly::dynamic content, tx::staged_mutation_type type)
    : doc_(std::move(doc)), content_(std::move(content)), type_(type)
{
}

tx::transaction_document &tx::staged_mutation::doc()
{
    return doc_;
}

const tx::staged_mutation_type &tx::staged_mutation::type() const
{
    return type_;
}

const folly::dynamic &tx::staged_mutation::content() const
{
    return content_;
}

bool tx::staged_mutation_queue::empty()
{
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.empty();
}

void tx::staged_mutation_queue::add(const tx::staged_mutation &mutation)
{
    std::unique_lock<std::mutex> lock(mutex_);
    queue_.push_back(mutation);
}
void tx::staged_mutation_queue::extract_to(const std::string &prefix, std::vector<couchbase::mutate_in_spec> &specs)
{
    std::unique_lock<std::mutex> lock(mutex_);
    auto inserts = folly::dynamic::array();
    auto replaces = folly::dynamic::array();
    auto removes = folly::dynamic::array();

    for (auto &mutation : queue_) {
        // clang-format off
        folly::dynamic doc = folly::dynamic::object
            (ATR_FIELD_PER_DOC_ID, mutation.doc().id())
            (ATR_FIELD_PER_DOC_CAS, std::to_string(mutation.doc().cas()))
            (ATR_FIELD_PER_DOC_BUCKET, mutation.doc().collection_ref().bucket_name())
            (ATR_FIELD_PER_DOC_SCOPE, mutation.doc().collection_ref().scope())
            (ATR_FIELD_PER_DOC_COLLECTION, mutation.doc().collection_ref().name())
        ;
        // clang-format on
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

tx::staged_mutation *tx::staged_mutation_queue::find_replace(couchbase::collection *collection, const std::string &id)
{
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto &item : queue_) {
        if (item.type() == staged_mutation_type::REPLACE && item.doc().id() == id &&
            item.doc().collection_ref().bucket_name() == collection->bucket_name() &&
            item.doc().collection_ref().scope() == collection->scope() && item.doc().collection_ref().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}

tx::staged_mutation *tx::staged_mutation_queue::find_insert(couchbase::collection *collection, const std::string &id)
{
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto &item : queue_) {
        if (item.type() == staged_mutation_type::INSERT && item.doc().id() == id &&
            item.doc().collection_ref().bucket_name() == collection->bucket_name() &&
            item.doc().collection_ref().scope() == collection->scope() && item.doc().collection_ref().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}

tx::staged_mutation *tx::staged_mutation_queue::find_remove(couchbase::collection *collection, const std::string &id)
{
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto &item : queue_) {
        if (item.type() == staged_mutation_type::REMOVE && item.doc().id() == id &&
            item.doc().collection_ref().bucket_name() == collection->bucket_name() &&
            item.doc().collection_ref().scope() == collection->scope() && item.doc().collection_ref().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}

void tx::staged_mutation_queue::commit()
{
    std::unique_lock<std::mutex> lock(mutex_);

    for (auto &item : queue_) {
        switch (item.type()) {
            case REMOVE:
                item.doc().collection_ref().remove(item.doc().id());
                break;
            case INSERT:
            case REPLACE:
                item.doc().collection_ref().mutate_in(item.doc().id(),
                                                      {
                                                          mutate_in_spec::upsert(TRANSACTION_INTERFACE_PREFIX_ONLY, nullptr).xattr(),
                                                          mutate_in_spec::fulldoc_upsert(item.content()),
                                                      });
                break;
        }
    }
}
