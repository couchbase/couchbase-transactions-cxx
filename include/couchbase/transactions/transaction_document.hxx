#pragma once

#include <couchbase/transactions/transaction_links.hxx>
#include <couchbase/transactions/transaction_document_status.hxx>
#include <couchbase/client/collection.hxx>
#include <ostream>

namespace couchbase
{
namespace transactions
{
    class transaction_document
    {
      private:
        collection &collection_;
        folly::dynamic value_;
        std::string id_;
        uint64_t cas_;
        transaction_links links_;
        transaction_document_status status_;

      public:
        transaction_document(collection &collection, std::string id, folly::dynamic value, uint64_t cas,
                             transaction_document_status status = NORMAL, transaction_links links = {});

        collection &collection_ref();
        [[nodiscard]] const folly::dynamic &content() const;
        [[nodiscard]] const std::string &id() const;
        [[nodiscard]] uint64_t cas() const;
        [[nodiscard]] transaction_links links() const;
        [[nodiscard]] transaction_document_status status() const;

        void content(const folly::dynamic &content);
        void cas(uint64_t cas);
        void status(transaction_document_status status);

        friend std::ostream &operator<<(std::ostream &os, const transaction_document &document);
    };
    std::ostream &operator<<(std::ostream &os, const transaction_document &document);
} // namespace transactions
} // namespace couchbase
