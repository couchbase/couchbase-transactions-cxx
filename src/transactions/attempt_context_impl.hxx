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
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/attempt_state.hxx>
#include <couchbase/transactions/transaction_get_result.hxx>

#include "atr_cleanup_entry.hxx"
#include "attempt_context_testing_hooks.hxx"
#include "transaction_context.hxx"
#include "exceptions_internal.hxx"

namespace couchbase
{
namespace transactions
{
    /**
     * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents, as well as commit or
     * rollback the transaction.
     */
    class transactions;
    enum class forward_compat_stage;
    class transaction_operation_failed;
    class staged_mutation_queue;
    class staged_mutation;

    class attempt_context_impl : public attempt_context
    {
      private:
        transaction_context& overall_;
        const transaction_config& config_;
        transactions* parent_;
        std::optional<couchbase::document_id> atr_id_;
        bool is_done_;
        std::unique_ptr<staged_mutation_queue> staged_mutations_;
        attempt_context_testing_hooks hooks_;
        std::list<transaction_operation_failed> errors_;
        // commit needs to access the hooks
        friend class staged_mutation_queue;
        // entry needs access to private members
        friend class atr_cleanup_entry;

        virtual transaction_get_result insert_raw(const couchbase::document_id& id, const std::string& content);

        virtual transaction_get_result replace_raw(const transaction_get_result& document, const std::string& content);

        template<typename V>
        V cache_error(std::function<V()> func)
        {
            existing_error();
            try {
                return func();
            } catch (const transaction_operation_failed& e) {
                errors_.push_back(e);
                throw;
            }
        }

        void existing_error();

        template<typename... Args>
        void trace(const std::string& fmt, Args... args)
        {
            txn_log->trace(attempt_format_string + fmt, this->transaction_id(), this->id(), args...);
        }

        template<typename... Args>
        void debug(const std::string& fmt, Args... args)
        {
            txn_log->debug(attempt_format_string + fmt, this->transaction_id(), this->id(), args...);
        }

        template<typename... Args>
        void info(const std::string& fmt, Args... args)
        {
            txn_log->info(attempt_format_string + fmt, this->transaction_id(), this->id(), args...);
        }

        template<typename... Args>
        void error(const std::string& fmt, Args... args)
        {
            txn_log->error(attempt_format_string + fmt, this->transaction_id(), this->id(), args...);
        }

        cluster& cluster_ref();

      public:
        attempt_context_impl(transactions* parent, transaction_context& transaction_ctx, const transaction_config& config);
        ~attempt_context_impl();

        virtual transaction_get_result get(const couchbase::document_id& id);
        virtual std::optional<transaction_get_result> get_optional(const couchbase::document_id& id);
        virtual void remove(transaction_get_result& document);
        virtual void commit();
        virtual void rollback();

        CB_NODISCARD bool is_done()
        {
            return is_done_;
        }

        CB_NODISCARD const std::string& transaction_id()
        {
            return overall_.transaction_id();
        }

        CB_NODISCARD const std::string& id()
        {
            return overall_.current_attempt().id;
        }

        CB_NODISCARD attempt_state state()
        {
            return overall_.current_attempt().state;
        }

        void state(attempt_state s)
        {
            overall_.current_attempt().state = s;
        }

        CB_NODISCARD const std::string atr_id()
        {
            return overall_.atr_id();
        }

        void atr_id(const std::string& atr_id)
        {
            overall_.atr_id(atr_id);
        }

        CB_NODISCARD const std::string atr_collection()
        {
            return overall_.atr_collection();
        }

        void atr_collection_name(const std::string& coll)
        {
            overall_.atr_collection(coll);
        }

        bool has_expired_client_side(std::string place, std::optional<const std::string> doc_id);

      private:
        bool expiry_overtime_mode_{ false };

        void check_expiry_pre_commit(std::string stage, std::optional<const std::string> doc_id);

        void check_expiry_during_commit_or_rollback(const std::string& stage, std::optional<const std::string> doc_id);

        void set_atr_pending_if_first_mutation(const couchbase::document_id& collection);

        void error_if_expired_and_not_in_overtime(const std::string& stage, std::optional<const std::string> doc_id);

        staged_mutation* check_for_own_write(const couchbase::document_id& id);

        void check_and_handle_blocking_transactions(const transaction_get_result& doc, forward_compat_stage stage);

        void check_atr_entry_for_blocking_document(const transaction_get_result& doc);

        void check_if_done();

        void atr_commit();

        void atr_commit_ambiguity_resolution();

        void atr_complete();

        void atr_abort();

        void atr_rollback_complete();

        void select_atr_if_needed(const couchbase::document_id& id);

        std::optional<transaction_get_result> do_get(const couchbase::document_id& id);

        std::optional<std::pair<transaction_get_result, result>> get_doc(const couchbase::document_id& id);

        couchbase::operations::mutate_in_request create_staging_request(const transaction_get_result& document, const std::string type);

        transaction_get_result create_staged_insert(const couchbase::document_id& id, const std::string& content, uint64_t& cas);
    };
} // namespace transactions
} // namespace couchbase
