#pragma once

#include <string>

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
        std::string staged_content_;

      public:
        transaction_links() = default;
        transaction_links(std::string atr_id, std::string atr_bucket_name, std::string atr_scope_name,
                          std::string atr_collection_name, std::string content, std::string version);

        const std::string &atr_id() const;
        const std::string &atr_bucket_name() const;
        const std::string &atr_scope_name() const;
        const std::string &atr_collection_name() const;
        const std::string &staged_version() const;
        const std::string &staged_content() const;

        /**
         * Note this doesn't guarantee an active transaction, as it may have expired and need rolling back.
         */
        bool is_document_in_transaction() const;
        bool is_document_being_removed() const;
        bool has_staged_write() const;
    };

} // namespace transactions
} // namespace couchbase
