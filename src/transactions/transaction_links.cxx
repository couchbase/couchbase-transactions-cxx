#include <utility>

#include <couchbase/transactions/transaction_links.hxx>
#include <couchbase/transactions/transaction_fields.hxx>

namespace tx = couchbase::transactions;

tx::transaction_links::transaction_links(std::string atr_id, std::string atr_bucket_name, std::string atr_scope_name,
                                         std::string atr_collection_name, folly::dynamic content, std::string version)
    : atr_id_(std::move(atr_id)), atr_bucket_name_(std::move(atr_bucket_name)), atr_scope_name_(std::move(atr_scope_name)),
      atr_collection_name_(std::move(atr_collection_name)), staged_content_(std::move(content)), staged_version_(std::move(version))
{
}

bool tx::transaction_links::is_document_in_transaction() const
{
    return !atr_id_.empty();
}

bool tx::transaction_links::is_document_being_removed() const
{
    return staged_content_ == STAGED_DATA_REMOVED_VALUE;
}

bool tx::transaction_links::has_staged_write() const
{
    return !staged_version_.empty();
}

const std::string &tx::transaction_links::atr_bucket_name() const
{
    return atr_bucket_name_;
}

const std::string &tx::transaction_links::atr_id() const
{
    return atr_id_;
}

const std::string &tx::transaction_links::atr_scope_name() const
{
    return atr_scope_name_;
}

const std::string &tx::transaction_links::atr_collection_name() const
{
    return atr_collection_name_;
}

const std::string &tx::transaction_links::staged_version() const
{
    return staged_version_;
}

const folly::dynamic &tx::transaction_links::staged_content() const
{
    return staged_content_;
}

std::ostream &couchbase::transactions::operator<<(std::ostream &os, const couchbase::transactions::transaction_links &links)
{
    os << "transaction_links{atr: " << links.atr_id_ << ", atr_bkt: " << links.atr_bucket_name_ << ", atr_scp: " << links.atr_scope_name_
       << ", atr_coll: " << links.atr_collection_name_ << ", ver: " << links.staged_version_ << "}";
    return os;
}
