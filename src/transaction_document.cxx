#include <string>

#include <libcouchbase/transactions/transaction_document.hxx>

couchbase::transactions::transaction_document::transaction_document(const collection &collection, const std::string &id,
                                                                    const std::string &value, uint64_t cas,
                                                                    couchbase::transactions::transaction_document_status status,
                                                                    couchbase::transactions::transaction_links links)
    : collection_(collection), id_(id), value_(value), cas_(cas), status_(status), links_(std::move(links))
{
}

const std::string &couchbase::transactions::transaction_document::content() const
{
    return value_;
}

const std::string &couchbase::transactions::transaction_document::id() const
{
    return id_;
}

const uint64_t couchbase::transactions::transaction_document::cas() const
{
    return cas_;
}

const couchbase::collection &couchbase::transactions::transaction_document::collection_ref() const
{
    return collection_;
}

const couchbase::transactions::transaction_links couchbase::transactions::transaction_document::links() const
{
    return links_;
}

const couchbase::transactions::transaction_document_status couchbase::transactions::transaction_document::status() const
{
    return status_;
}

void couchbase::transactions::transaction_document::status(couchbase::transactions::transaction_document_status status)
{
    status_ = status;
}

void couchbase::transactions::transaction_document::content(const std::string &content)
{
    value_ = content;
}

void couchbase::transactions::transaction_document::cas(uint64_t cas)
{
    cas_ = cas;
}
