#include <string>

#include <libcouchbase/transactions/transaction_document.hxx>

couchbase::transactions::TransactionDocument::TransactionDocument(const Collection &collection, const std::string &id,
                                                                  const std::string &value, uint64_t cas,
                                                                  couchbase::transactions::TransactionDocumentStatus status,
                                                                  couchbase::transactions::TransactionLinks links)
    : collection_(collection), id_(id), value_(value), cas_(cas), status_(status), links_(std::move(links))
{
}

const std::string &couchbase::transactions::TransactionDocument::content() const
{
    return value_;
}

const std::string &couchbase::transactions::TransactionDocument::id() const
{
    return id_;
}

const uint64_t couchbase::transactions::TransactionDocument::cas() const
{
    return cas_;
}

const couchbase::Collection &couchbase::transactions::TransactionDocument::collection() const
{
    return collection_;
}

const couchbase::transactions::TransactionLinks couchbase::transactions::TransactionDocument::links() const
{
    return links_;
}

const couchbase::transactions::TransactionDocumentStatus couchbase::transactions::TransactionDocument::status() const
{
    return status_;
}

void couchbase::transactions::TransactionDocument::status(couchbase::transactions::TransactionDocumentStatus status)
{
    status_ = status;
}

void couchbase::transactions::TransactionDocument::content(const std::string &content)
{
    value_ = content;
}

void couchbase::transactions::TransactionDocument::cas(uint64_t cas)
{
    cas_ = cas;
}
