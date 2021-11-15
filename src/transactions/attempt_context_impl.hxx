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

#include <couchbase/transactions/async_attempt_context.hxx>
#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/attempt_state.hxx>
#include <couchbase/transactions/transaction_get_result.hxx>

#include "atr_cleanup_entry.hxx"
#include "attempt_context_testing_hooks.hxx"
#include "error_list.hxx"
#include "exceptions_internal.hxx"
#include "transaction_context.hxx"
#include "waitable_op_list.hxx"

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
    class staged_mutation_queue;
    class staged_mutation;

    class attempt_context_impl
      : public attempt_context
      , public async_attempt_context
    {
      private:
        transaction_context& overall_;
        const transaction_config& config_;
        std::optional<couchbase::document_id> atr_id_;
        bool is_done_;
        std::unique_ptr<staged_mutation_queue> staged_mutations_;
        attempt_context_testing_hooks hooks_;
        error_list errors_;
        std::mutex mutex_;
        waitable_op_list op_list_;
        std::atomic<bool> blocking_mode_;

        // commit needs to access the hooks
        friend class staged_mutation_queue;
        // entry needs access to private members
        friend class atr_cleanup_entry;

        virtual transaction_get_result insert_raw(const couchbase::document_id& id, const std::string& content);
        virtual void insert_raw(const couchbase::document_id& id, const std::string& content, Callback&& cb);

        virtual transaction_get_result replace_raw(const transaction_get_result& document, const std::string& content);
        virtual void replace_raw(const transaction_get_result& document, const std::string& content, Callback&& cb);

        template<typename Cb>
        void op_completed_with_callback(Cb&& cb, std::optional<transaction_get_result> t)
        {
            try {
                cb(std::nullopt, t);
                op_list_.change_count(-1);
            } catch (const transaction_operation_failed& ex) {
                txn_log->error("op callback called a txn operation that threw exception {}", ex.what());
                op_list_.change_count(-1);
                // presumably that op called op_completed_with_error already, so
                // don't do anything here but swallow it.
            } catch (const async_operation_conflict& op_ex) {
                // the count isn't changed when this is thrown, so just swallow it and log
                txn_log->error("op callback called a txn operation that threw exception {}", op_ex.what());
            } catch (const std::exception& e) {
                // if the callback throws something which wasn't handled
                // we just want to handle as a rollback
                txn_log->error("op callback threw exception {}", e.what());
                errors_.push_back(transaction_operation_failed(FAIL_OTHER, e.what()));
                op_list_.change_count(-1);
            }
        }

        template<typename Cb>
        void op_completed_with_callback(Cb&& cb)
        {
            try {
                cb(std::nullopt);
                op_list_.change_count(-1);
            } catch (const async_operation_conflict& op_ex) {
                // the count isn't changed when this is thrown, so just swallow it and log
                txn_log->error("op callback called a txn operation that threw exception {}", op_ex.what());
            } catch (const transaction_operation_failed& ex) {
                txn_log->error("op callback called a txn operation that threw exception {}", ex.what());
                op_list_.change_count(-1);
            } catch (const std::exception& e) {
                txn_log->error("op callback threw exception {}", e.what());
                errors_.push_back(transaction_operation_failed(FAIL_OTHER, e.what()));
                op_list_.change_count(-1);
            }
        }
        void op_completed_with_error(
          std::function<void(std::optional<transaction_operation_failed>, std::optional<transaction_get_result>)> cb,
          transaction_operation_failed err)
        {
            errors_.push_back(std::move(err));
            cb({ err }, std::nullopt);
            op_list_.change_count(-1);
        }
        void op_completed_with_error(std::function<void(std::optional<transaction_operation_failed>)> cb, transaction_operation_failed err)
        {
            errors_.push_back(std::move(err));
            cb(err);
            op_list_.change_count(-1);
        }

        template<typename Handler>
        void cache_error_async(Handler&& cb, std::function<void()> func)
        {
            try {
                op_list_.change_count(1);
                existing_error();
                return func();
            } catch (const async_operation_conflict& e) {
                // can't do anything here but log and eat it.
                error("Attempted to perform txn operation after commit/rollback started");
                op_completed_with_error(cb, transaction_operation_failed(FAIL_OTHER, e.what()));
            } catch (const transaction_operation_failed& e) {
                // thrown only from call_func when previous error exists, so eat it.
            } catch (const std::exception& e) {
                op_completed_with_error(cb, transaction_operation_failed(FAIL_OTHER, e.what()));
            }
        }

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
        attempt_context_impl(transaction_context& transaction_ctx);
        ~attempt_context_impl();

        virtual transaction_get_result get(const couchbase::document_id& id);
        virtual void get(const couchbase::document_id& id, Callback&& cb);

        virtual std::optional<transaction_get_result> get_optional(const couchbase::document_id& id);
        virtual void get_optional(const couchbase::document_id& id, Callback&& cb);

        virtual void remove(transaction_get_result& document);
        virtual void remove(transaction_get_result& document, VoidCallback&& cb);
        virtual void commit();
        virtual void commit(VoidCallback&& cb)
        {
            // not implemented yet
        }
        virtual void rollback();
        virtual void rollback(VoidCallback&& cb)
        {
            // not implemented yet
        }
        void existing_error(bool prev_op_failed = true)
        {
            if (!errors_.empty()) {
                errors_.do_throw((prev_op_failed ? std::make_optional(PREVIOUS_OPERATION_FAILED) : std::nullopt));
            }
        }

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

        bool check_expiry_pre_commit(std::string stage, std::optional<const std::string> doc_id);

        void check_expiry_during_commit_or_rollback(const std::string& stage, std::optional<const std::string> doc_id);

        template<typename Handler>
        void set_atr_pending_locked(const couchbase::document_id& collection, std::unique_lock<std::mutex>&& lock, Handler&& cb);

        std::optional<error_class> error_if_expired_and_not_in_overtime(const std::string& stage, std::optional<const std::string> doc_id);

        staged_mutation* check_for_own_write(const couchbase::document_id& id);

        template<typename Handler>
        void check_and_handle_blocking_transactions(const transaction_get_result& doc, forward_compat_stage stage, Handler&& cb);

        template<typename Handler, typename Delay>
        void check_atr_entry_for_blocking_document(const transaction_get_result& doc, Delay delay, Handler&& cb);

        template<typename Handler>
        void check_if_done(Handler& cb);

        void atr_commit();

        void atr_commit_ambiguity_resolution();

        void atr_complete();

        void atr_abort();

        void atr_rollback_complete();

        void select_atr_if_needed_unlocked(const couchbase::document_id& id,
                                           std::function<void(std::optional<transaction_operation_failed>)>&& cb);

        template<typename Handler>
        void do_get(const couchbase::document_id& id, Handler&& cb);

        void get_doc(const couchbase::document_id& id,
                     std::function<void(std::optional<error_class>, std::optional<transaction_get_result>)>&& cb);

        couchbase::operations::mutate_in_request create_staging_request(const transaction_get_result& document, const std::string type);

        template<typename Handler>
        void create_staged_insert(const couchbase::document_id& id, const std::string& content, uint64_t cas, Handler&& cb);
    };
} // namespace transactions
} // namespace couchbase
