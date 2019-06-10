#include <string>

#include <libcouchbase/transactions/transaction_document.hxx>

couchbase::transactions::TransactionDocument::TransactionDocument(std::string value) : value_(value), id_(""), cas_(0)
{
}

const std::string couchbase::transactions::TransactionDocument::content() const
{
    return value_;
}

const std::string couchbase::transactions::TransactionDocument::id() const
{
    return id_;
}

const uint64_t couchbase::transactions::TransactionDocument::cas() const
{
    return cas_;
}
