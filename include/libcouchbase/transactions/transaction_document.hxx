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
        std::string value_;
        std::string id_;
        uint64_t cas_;
        transaction_links links_;
        transaction_document_status status_;

      public:
        transaction_document(const collection &collection, const std::string &id, const std::string &value, uint64_t cas,
                             transaction_document_status status = NORMAL, transaction_links links = {});

        const collection &collection_ref() const;
        const std::string &content() const;
        void content(const std::string &content);
        const std::string &id() const;
        const uint64_t cas() const;
        void cas(uint64_t cas);
        const transaction_links links() const;
        const transaction_document_status status() const;
        void status(transaction_document_status status);

        template <typename ValueParser> typename ValueParser::ValueType content_as()
        {
            return ValueParser::parse(value_);
        }
    };
} // namespace transactions
} // namespace couchbase
