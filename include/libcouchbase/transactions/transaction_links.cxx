#include <libcouchbase/transactions/transaction_links.hxx>
#include "transaction_fields.hxx"

couchbase::transactions::TransactionLinks::TransactionLinks(const std::string &atr_id, const std::string &atr_bucket_name,
                                                            const std::string &atr_scope_name, const std::string &atr_collection_name,
                                                            const std::string &content, const std::string &version)
    : atr_id_(atr_id), atr_bucket_name_(atr_bucket_name), atr_scope_name_(atr_scope_name), atr_collection_name_(atr_collection_name),
      staged_content_(content), staged_version_(version)
{
}

bool couchbase::transactions::TransactionLinks::is_document_in_transaction() const
{
    return !atr_id_.empty();
}

bool couchbase::transactions::TransactionLinks::is_document_being_removed() const
{
    return staged_content_ == STAGED_DATA_REMOVED_VALUE;
}

bool couchbase::transactions::TransactionLinks::has_staged_write() const
{
    return !staged_version_.empty();
}

const std::string &couchbase::transactions::TransactionLinks::atr_bucket_name() const
{
    return atr_bucket_name_;
}

const std::string &couchbase::transactions::TransactionLinks::atr_id() const
{
    return atr_id_;
}

const std::string &couchbase::transactions::TransactionLinks::atr_scope_name() const
{
    return atr_scope_name_;
}

const std::string &couchbase::transactions::TransactionLinks::atr_collection_name() const
{
    return atr_collection_name_;
}

const std::string &couchbase::transactions::TransactionLinks::staged_version() const
{
    return staged_version_;
}

const std::string &couchbase::transactions::TransactionLinks::staged_content() const
{
    return staged_content_;
}
