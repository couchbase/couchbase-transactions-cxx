/*
 *     Copyright 2020 Couchbase, Inc.
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

#include <boost/optional.hpp>

#include <nlohmann/json.hpp>

#include <couchbase/transactions/active_transaction_record.hxx>
#include <couchbase/transactions/transaction_fields.hxx>

namespace couchbase
{
namespace transactions
{
    class transaction_links
    {
      private:
        boost::optional<std::string> atr_id_;
        boost::optional<std::string> atr_bucket_name_;
        boost::optional<std::string> atr_scope_name_;
        boost::optional<std::string> atr_collection_name_;
        // id of the transaction that has staged content
        boost::optional<std::string> staged_transaction_id_;
        boost::optional<std::string> staged_attempt_id_;
        boost::optional<nlohmann::json> staged_content_;

        // for {BACKUP_FIELDS}
        boost::optional<std::string> cas_pre_txn_;
        boost::optional<std::string> revid_pre_txn_;
        boost::optional<uint32_t> exptime_pre_txn_;
        boost::optional<std::string> op_;

      public:
        transaction_links() = default;
        transaction_links(boost::optional<std::string> atr_id,
                          boost::optional<std::string> atr_bucket_name,
                          boost::optional<std::string> atr_scope_name,
                          boost::optional<std::string> atr_collection_name,
                          boost::optional<std::string> staged_transaction_id,
                          boost::optional<std::string> staged_attempt_id,
                          boost::optional<nlohmann::json> staged_content,
                          boost::optional<std::string> cas_pre_txn,
                          boost::optional<std::string> revid_pre_txn,
                          boost::optional<uint32_t> exptime_pre_txn,
                          boost::optional<std::string> op)
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
          , op_(std::move(op))
        {
        }

        /**
         * Note this doesn't guarantee an active transaction, as it may have expired and need rolling back.
         */
        CB_NODISCARD bool is_document_in_transaction() const
        {
            return !!(atr_id_);
        }

        CB_NODISCARD bool is_document_being_removed() const
        {
            return staged_content_ && *staged_content_ == REMOVE_SENTINEL;
        }

        CB_NODISCARD bool has_staged_write() const
        {
            return !!(staged_attempt_id_);
        }

        CB_NODISCARD boost::optional<std::string> atr_id() const
        {
            return atr_id_;
        }

        CB_NODISCARD boost::optional<std::string> atr_bucket_name() const
        {
            return atr_bucket_name_;
        }

        CB_NODISCARD boost::optional<std::string> atr_scope_name() const
        {
            return atr_scope_name_;
        }

        CB_NODISCARD boost::optional<std::string> atr_collection_name() const
        {
            return atr_collection_name_;
        }

        CB_NODISCARD boost::optional<std::string> staged_transaction_id() const
        {
            return staged_transaction_id_;
        }

        CB_NODISCARD boost::optional<std::string> staged_attempt_id() const
        {
            return staged_attempt_id_;
        }

        CB_NODISCARD boost::optional<std::string> cas_pre_txn() const
        {
            return cas_pre_txn_;
        }

        CB_NODISCARD boost::optional<std::string> revid_pre_txn() const
        {
            return revid_pre_txn_;
        }

        CB_NODISCARD boost::optional<uint32_t> exptime_pre_txn() const
        {
            return exptime_pre_txn_;
        }

        CB_NODISCARD boost::optional<std::string> op() const
        {
            return op_;
        }

        template<typename Content>
        CB_NODISCARD Content staged_content() const
        {
            return staged_content_ ? staged_content_->get<Content>() : Content();
        }

        friend std::ostream& operator<<(std::ostream& os, const transaction_links& links);
    };

    std::ostream& operator<<(std::ostream& os, const transaction_links& links);
} // namespace transactions
} // namespace couchbase
