#pragma once

#include <string>
#include <folly/dynamic.h>

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
        folly::dynamic staged_content_;

      public:
        transaction_links() = default;
        transaction_links(std::string atr_id, std::string atr_bucket_name, std::string atr_scope_name, std::string atr_collection_name,
                          folly::dynamic content, std::string version);

        [[nodiscard]] const std::string &atr_id() const;
        [[nodiscard]] const std::string &atr_bucket_name() const;
        [[nodiscard]] const std::string &atr_scope_name() const;
        [[nodiscard]] const std::string &atr_collection_name() const;
        [[nodiscard]] const std::string &staged_version() const;
        [[nodiscard]] const folly::dynamic &staged_content() const;

        /**
         * Note this doesn't guarantee an active transaction, as it may have expired and need rolling back.
         */
        [[nodiscard]] bool is_document_in_transaction() const;
        [[nodiscard]] bool is_document_being_removed() const;
        [[nodiscard]] bool has_staged_write() const;
    };
} // namespace transactions
} // namespace couchbase
