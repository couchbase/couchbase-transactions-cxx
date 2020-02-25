#pragma once

#include <nlohmann/json.hpp>
#include <ostream>
#include <string>

#include "transaction_fields.hxx"

namespace couchbase
{
namespace transactions
{
    class transaction_links
    {
      private:
        std::string atr_id_;
        std::string atr_bucket_name_;
        std::string atr_scope_name_;
        std::string atr_collection_name_;
        // id of the transaction that has staged content
        std::string staged_version_;
        nlohmann::json staged_content_;

      public:
        transaction_links() = default;
        template<typename Content>
        transaction_links(std::string atr_id,
                          std::string atr_bucket_name,
                          std::string atr_scope_name,
                          std::string atr_collection_name,
                          Content content,
                          std::string version)
          : atr_id_(std::move(atr_id))
          , atr_bucket_name_(std::move(atr_bucket_name))
          , atr_scope_name_(std::move(atr_scope_name))
          , atr_collection_name_(std::move(atr_collection_name))
          , staged_content_(std::move(content))
          , staged_version_(std::move(version))
        {
        }

        [[nodiscard]] const std::string& atr_id() const
        {
            return atr_id_;
        }

        [[nodiscard]] const std::string& atr_bucket_name() const
        {
            return atr_bucket_name_;
        }

        [[nodiscard]] const std::string& atr_scope_name() const
        {
            return atr_scope_name_;
        }

        [[nodiscard]] const std::string& atr_collection_name() const
        {
            return atr_collection_name_;
        }

        [[nodiscard]] const std::string& staged_version() const
        {
            return staged_version_;
        }

        template<typename Content>
        [[nodiscard]] Content staged_content() const
        {
            return staged_content_.get<Content>();
        }

        /**
         * Note this doesn't guarantee an active transaction, as it may have expired and need rolling back.
         */
        [[nodiscard]] bool is_document_in_transaction() const
        {
            return !atr_id_.empty();
        }

        [[nodiscard]] bool is_document_being_removed() const
        {
            return staged_content_ == STAGED_DATA_REMOVED_VALUE;
        }

        [[nodiscard]] bool has_staged_write() const
        {
            return !staged_version_.empty();
        }

        friend std::ostream& operator<<(std::ostream& os, const transaction_links& links);
    };

    std::ostream& operator<<(std::ostream& os, const transaction_links& links);
} // namespace transactions
} // namespace couchbase
