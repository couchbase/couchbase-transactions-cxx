#pragma once

#include <couchbase/client/collection.hxx>
#include <couchbase/transactions/transaction_document_status.hxx>
#include <couchbase/transactions/transaction_links.hxx>
#include <nlohmann/json.hpp>
#include <ostream>

namespace couchbase
{
namespace transactions
{
    class transaction_document
    {
      private:
        collection& collection_;
        nlohmann::json value_;
        std::string id_;
        uint64_t cas_;
        transaction_links links_;
        transaction_document_status status_;

      public:
        template<typename Content>
        transaction_document(collection& collection,
                             std::string id,
                             Content value,
                             uint64_t cas,
                             transaction_document_status status = NORMAL,
                             transaction_links links = {})
          : collection_(collection)
          , value_(std::move(value))
          , id_(std::move(id))
          , cas_(cas)
          , status_(status)
          , links_(links)
        {
        }

        collection& collection_ref()
        {
            return collection_;
        }

        template<typename Content>
        [[nodiscard]] Content content() const
        {
            return value_.get<Content>();
        }

        [[nodiscard]] const std::string& id() const
        {
            return id_;
        }

        [[nodiscard]] uint64_t cas() const
        {
            return cas_;
        }

        [[nodiscard]] transaction_links links() const
        {
            return links_;
        }

        [[nodiscard]] transaction_document_status status() const
        {
            return status_;
        }

        template<typename Content>
        void content(const Content& content)
        {
            value_ = content;
        }

        void cas(uint64_t cas)
        {
            cas_ = cas;
        }

        void status(transaction_document_status status)
        {
            status_ = status;
        }

        friend std::ostream& operator<<(std::ostream& os, const transaction_document& document);
    };

    std::ostream& operator<<(std::ostream& os, const transaction_document& document);
} // namespace transactions
} // namespace couchbase
