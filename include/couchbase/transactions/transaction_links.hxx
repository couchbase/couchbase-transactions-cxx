/*
 *     Copyright 2021 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

#include <ostream>
#include <string>

#include <optional>

#include <couchbase/internal/nlohmann/json.hpp>
#include <couchbase/support.hxx>

namespace couchbase
{
namespace transactions
{
    /** @internal */
    class transaction_links
    {
      private:
        std::optional<std::string> atr_id_;
        std::optional<std::string> atr_bucket_name_;
        std::optional<std::string> atr_scope_name_;
        std::optional<std::string> atr_collection_name_;
        // id of the transaction that has staged content
        std::optional<std::string> staged_transaction_id_;
        std::optional<std::string> staged_attempt_id_;
        std::optional<std::string> staged_content_;

        // for {BACKUP_FIELDS}
        std::optional<std::string> cas_pre_txn_;
        std::optional<std::string> revid_pre_txn_;
        std::optional<uint32_t> exptime_pre_txn_;
        std::optional<std::string> crc32_of_staging_;
        std::optional<std::string> op_;
        std::optional<nlohmann::json> forward_compat_;
        bool is_deleted_;

      public:
        transaction_links() = default;
        transaction_links(std::optional<std::string> atr_id,
                          std::optional<std::string> atr_bucket_name,
                          std::optional<std::string> atr_scope_name,
                          std::optional<std::string> atr_collection_name,
                          std::optional<std::string> staged_transaction_id,
                          std::optional<std::string> staged_attempt_id,
                          std::optional<std::string> staged_content,
                          std::optional<std::string> cas_pre_txn,
                          std::optional<std::string> revid_pre_txn,
                          std::optional<uint32_t> exptime_pre_txn,
                          std::optional<std::string> crc32_of_staging,
                          std::optional<std::string> op,
                          std::optional<nlohmann::json> forward_compat,
                          bool is_deleted)
          : atr_id_(std::move(atr_id))
          , atr_bucket_name_(std::move(atr_bucket_name))
          , atr_scope_name_(std::move(atr_scope_name))
          , atr_collection_name_(std::move(atr_collection_name))
          , staged_transaction_id_(std::move(staged_transaction_id))
          , staged_attempt_id_(std::move(staged_attempt_id))
          , staged_content_(std::move(staged_content))
          , cas_pre_txn_(std::move(cas_pre_txn))
          , revid_pre_txn_(std::move(revid_pre_txn))
          , exptime_pre_txn_(exptime_pre_txn)
          , crc32_of_staging_(std::move(crc32_of_staging))
          , op_(std::move(op))
          , forward_compat_(forward_compat)
          , is_deleted_(is_deleted)
        {
        }

        /**
         * Note this doesn't guarantee an active transaction, as it may have expired and need rolling back.
         */
        CB_NODISCARD bool is_document_in_transaction() const
        {
            return !!(atr_id_);
        }
        CB_NODISCARD bool has_staged_content() const
        {
            return !!(staged_content_);
        }
        CB_NODISCARD bool is_document_being_removed() const
        {
            return (!!op_ && *op_ == "remove");
        }

        CB_NODISCARD bool has_staged_write() const
        {
            return !!(staged_attempt_id_);
        }

        CB_NODISCARD std::optional<std::string> atr_id() const
        {
            return atr_id_;
        }

        CB_NODISCARD std::optional<std::string> atr_bucket_name() const
        {
            return atr_bucket_name_;
        }

        CB_NODISCARD std::optional<std::string> atr_scope_name() const
        {
            return atr_scope_name_;
        }

        CB_NODISCARD std::optional<std::string> atr_collection_name() const
        {
            return atr_collection_name_;
        }

        CB_NODISCARD std::optional<std::string> staged_transaction_id() const
        {
            return staged_transaction_id_;
        }

        CB_NODISCARD std::optional<std::string> staged_attempt_id() const
        {
            return staged_attempt_id_;
        }

        CB_NODISCARD std::optional<std::string> cas_pre_txn() const
        {
            return cas_pre_txn_;
        }

        CB_NODISCARD std::optional<std::string> revid_pre_txn() const
        {
            return revid_pre_txn_;
        }

        CB_NODISCARD std::optional<uint32_t> exptime_pre_txn() const
        {
            return exptime_pre_txn_;
        }

        CB_NODISCARD std::optional<std::string> op() const
        {
            return op_;
        }

        CB_NODISCARD std::optional<std::string> crc32_of_staging() const
        {
            return crc32_of_staging_;
        }

        CB_NODISCARD std::string staged_content() const
        {
            return staged_content_ ? *staged_content_ : "";
        }

        CB_NODISCARD std::optional<nlohmann::json> forward_compat() const
        {
            return forward_compat_;
        }

        CB_NODISCARD bool is_deleted() const
        {
            return is_deleted_;
        }

        friend std::ostream& operator<<(std::ostream& os, const transaction_links& links);
    };

    std::ostream& operator<<(std::ostream& os, const transaction_links& links);
} // namespace transactions
} // namespace couchbase
