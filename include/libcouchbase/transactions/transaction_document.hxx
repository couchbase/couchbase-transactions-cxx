#pragma once

#include <libcouchbase/transactions/transaction_links.hxx>
#include <libcouchbase/transactions/transaction_document_status.hxx>
#include <libcouchbase/collection.hxx>

namespace couchbase
{
namespace transactions
{
    class TransactionDocument
    {
      private:
        const Collection &collection_;
        std::string value_;
        std::string id_;
        uint64_t cas_;
        TransactionLinks links_;
        TransactionDocumentStatus status_;

      public:
        TransactionDocument(const Collection &collection, const std::string &id, const std::string &value, uint64_t cas,
                            TransactionDocumentStatus status = NORMAL, TransactionLinks links = {});

        const Collection &collection() const;
        const std::string &content() const;
        void content(const std::string &content);
        const std::string &id() const;
        const uint64_t cas() const;
        void cas(uint64_t cas);
        const TransactionLinks links() const;
        const TransactionDocumentStatus status() const;
        void status(TransactionDocumentStatus status);

        template < typename ValueParser > typename ValueParser::ValueType content_as()
        {
            return ValueParser::parse(value_);
        }
    };
} // namespace transactions
} // namespace couchbase
