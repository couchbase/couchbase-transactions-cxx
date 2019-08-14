#include <string>

#include <libcouchbase/transactions/transaction_document.hxx>
#include <utility>

couchbase::transactions::transaction_document::transaction_document(collection &collection, std::string id, json11::Json value,
                                                                    uint64_t cas,
                                                                    couchbase::transactions::transaction_document_status status,
                                                                    couchbase::transactions::transaction_links links)
    : collection_(collection), value_(std::move(value)), id_(std::move(id)), cas_(cas), status_(status), links_(std::move(links))
{
}

const json11::Json &couchbase::transactions::transaction_document::content() const
{
    return value_;
}

const std::string &couchbase::transactions::transaction_document::id() const
{
    return id_;
}

uint64_t couchbase::transactions::transaction_document::cas() const
{
    return cas_;
}

couchbase::collection &couchbase::transactions::transaction_document::collection_ref()
{
    return collection_;
}

couchbase::transactions::transaction_links couchbase::transactions::transaction_document::links() const
{
    return links_;
}

couchbase::transactions::transaction_document_status couchbase::transactions::transaction_document::status() const
{
    return status_;
}

void couchbase::transactions::transaction_document::status(couchbase::transactions::transaction_document_status status)
{
    status_ = status;
}

void couchbase::transactions::transaction_document::content(const json11::Json &content)
{
    value_ = content;
}

void couchbase::transactions::transaction_document::cas(uint64_t cas)
{
    cas_ = cas;
}
