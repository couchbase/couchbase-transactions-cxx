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

#include <chrono>
#include <string>
#include <thread>
#include <vector>

#include "transaction_attempt.hxx"
#include "transactions_cleanup.hxx"
#include <couchbase/transactions.hxx>
#include <couchbase/transactions/async_attempt_context.hxx>
#include <couchbase/transactions/transaction_config.hxx>
#include <couchbase/transactions/transaction_result.hxx>

namespace couchbase
{
namespace transactions
{
    class attempt_context_impl;

    class transaction_context
    {
      public:
        transaction_context(transactions& txns);

        CB_NODISCARD const std::string& transaction_id() const
        {
            return transaction_id_;
        }

        CB_NODISCARD size_t num_attempts() const
        {
            return attempts_.size();
        }
        CB_NODISCARD const std::vector<transaction_attempt>& attempts() const
        {
            return attempts_;
        }
        CB_NODISCARD std::vector<transaction_attempt>& attempts()
        {
            return const_cast<std::vector<transaction_attempt>&>(const_cast<const transaction_context*>(this)->attempts());
        }
        CB_NODISCARD const transaction_attempt& current_attempt() const
        {
            if (attempts_.empty()) {
                throw std::runtime_error("transaction context has no attempts yet");
            }
            return attempts_.back();
        }
        CB_NODISCARD transaction_attempt& current_attempt()
        {
            return const_cast<transaction_attempt&>(const_cast<const transaction_context*>(this)->current_attempt());
        }

        void add_attempt();

        CB_NODISCARD couchbase::cluster& cluster_ref()
        {
            return transactions_.cluster_ref();
        }

        transaction_config& config()
        {
            return config_;
        }

        const transaction_config& config() const
        {
            return config_;
        }

        transactions_cleanup& cleanup()
        {
            return cleanup_;
        }

        // TODO: do we need to pass in the config?  Once per-txn config is implemented, possibly not needed.
        CB_NODISCARD bool has_expired_client_side(const transaction_config& config);

        void retry_delay();

        CB_NODISCARD std::chrono::time_point<std::chrono::steady_clock> start_time_client() const
        {
            return start_time_client_;
        }

        CB_NODISCARD const std::string atr_id() const
        {
            return atr_id_;
        }

        void atr_id(const std::string& id)
        {
            atr_id_ = id;
        }
        CB_NODISCARD std::string atr_collection() const
        {
            return atr_collection_;
        }
        void atr_collection(const std::string& coll)
        {
            atr_collection_ = coll;
        }

        CB_NODISCARD transaction_result get_transaction_result() const
        {
            return transaction_result{ transaction_id(), current_attempt().state == attempt_state::COMPLETED };
        }

        void new_attempt_context();

        std::shared_ptr<attempt_context_impl> current_attempt_context();

        // These functions just delegate to the current_attempt_context_
        void get(const couchbase::document_id& id, async_attempt_context::Callback&& cb);

        void get_optional(const couchbase::document_id& id, async_attempt_context::Callback&& cb);

        void insert(const couchbase::document_id& id, const std::string& content, async_attempt_context::Callback&& cb);

        void replace(const transaction_get_result& doc, const std::string& content, async_attempt_context::Callback&& cb);

        void remove(const transaction_get_result& doc, async_attempt_context::VoidCallback&& cb);

        void query(const std::string& statement, const transaction_query_options& opts, async_attempt_context::QueryCallback&& cb);

        void commit(async_attempt_context::VoidCallback&& cb);

        void rollback(async_attempt_context::VoidCallback&& cb);

        void existing_error();

        // TODO: do we need to pass in the config?  Once per-txn config is implemented, possibly not needed.
        std::chrono::nanoseconds remaining(const transaction_config& config) const;

      private:
        std::string transaction_id_;

        /** The time this overall transaction started */
        const std::chrono::time_point<std::chrono::steady_clock> start_time_client_;

        transaction_config config_;

        transactions& transactions_;

        /**
         * Will be non-zero only when resuming a deferred transaction. It records how much time has elapsed in total in the deferred
         * transaction, including the time spent in the original transaction plus any time spent while deferred.
         */
        const std::chrono::nanoseconds deferred_elapsed_;

        std::vector<transaction_attempt> attempts_;
        std::string atr_id_;
        std::string atr_collection_;
        transactions_cleanup& cleanup_;
        std::shared_ptr<attempt_context_impl> current_attempt_context_;
    };
} // namespace transactions
} // namespace couchbase
