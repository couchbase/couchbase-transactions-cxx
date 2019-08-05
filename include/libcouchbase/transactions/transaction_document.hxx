#pragma once

#include <libcouchbase/transactions/transaction_links.hxx>
#include <libcouchbase/transactions/transaction_document_status.hxx>
#include <libcouchbase/collection.hxx>

namespace couchbase
{
namespace transactions
{
    class transaction_document
    {
      private:
        const collection &collection_;
        json11::Json value_;
        std::string id_;
        uint64_t cas_;
        transaction_links links_;
        transaction_document_status status_;

      public:
        transaction_document(const collection &collection, std::string id, json11::Json value, uint64_t cas,
                             transaction_document_status status = NORMAL, transaction_links links = {});

        const collection &collection_ref() const;
        const json11::Json &content() const;
        void content(const json11::Json &content);
        const std::string &id() const;
        uint64_t cas() const;
        void cas(uint64_t cas);
        transaction_links links() const;
        transaction_document_status status() const;
        void status(transaction_document_status status);
    };
} // namespace transactions
} // namespace couchbase
