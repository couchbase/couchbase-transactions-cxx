#include <string>

#include <couchbase/transactions/transaction_document.hxx>
#include <utility>

namespace tx = couchbase::transactions;

tx::transaction_document::transaction_document(collection &collection, std::string id, json11::Json value, uint64_t cas,
                                               tx::transaction_document_status status, tx::transaction_links links)
    : collection_(collection), value_(std::move(value)), id_(std::move(id)), cas_(cas), status_(status), links_(std::move(links))
{
}

const json11::Json &tx::transaction_document::content() const
{
    return value_;
}

const std::string &tx::transaction_document::id() const
{
    return id_;
}

uint64_t tx::transaction_document::cas() const
{
    return cas_;
}

couchbase::collection &tx::transaction_document::collection_ref()
{
    return collection_;
}

tx::transaction_links tx::transaction_document::links() const
{
    return links_;
}

tx::transaction_document_status tx::transaction_document::status() const
{
    return status_;
}

void tx::transaction_document::status(tx::transaction_document_status status)
{
    status_ = status;
}

void tx::transaction_document::content(const json11::Json &content)
{
    value_ = content;
}

void tx::transaction_document::cas(uint64_t cas)
{
    cas_ = cas;
}
