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

#include "attempt_context_testing_hooks.hxx"
#include "couchbase/transactions/internal/atr_cleanup_entry.hxx"
#include "couchbase/transactions/internal/exceptions_internal.hxx"
#include "couchbase/transactions/internal/transaction_context.hxx"
#include "error_list.hxx"
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
        std::optional<couchbase::document_id> atr_id_;
        bool is_done_;
        std::unique_ptr<staged_mutation_queue> staged_mutations_;
        attempt_context_testing_hooks& hooks_;
        error_list errors_;
        std::mutex mutex_;
        waitable_op_list op_list_;

        // commit needs to access the hooks
        friend class staged_mutation_queue;
        // entry needs access to private members
        friend class atr_cleanup_entry;
        // transaction_context needs access to the two functions below
        friend class transaction_context;

        virtual transaction_get_result insert_raw(const couchbase::document_id& id, const std::string& content);
        virtual void insert_raw(const couchbase::document_id& id, const std::string& content, Callback&& cb);

        virtual transaction_get_result replace_raw(const transaction_get_result& document, const std::string& content);
        virtual void replace_raw(const transaction_get_result& document, const std::string& content, Callback&& cb);

        void remove_staged_insert(const couchbase::document_id& id, VoidCallback&& cb);

        // These are all just stubs for now
        void get_with_query(const couchbase::document_id& id, bool optional, Callback&& cb);
        void insert_raw_with_query(const couchbase::document_id& id, const std::string& content, Callback&& cb);
        void replace_raw_with_query(const transaction_get_result& document, const std::string& content, Callback&& cb);
        void remove_with_query(const transaction_get_result& document, VoidCallback&& cb);

        void commit_with_query(VoidCallback&& cb);
        void rollback_with_query(VoidCallback&& cb);

        template<typename Handler>
        void query_begin_work(Handler&& cb);

        void do_query(const std::string& statement, const transaction_query_options& opts, QueryCallback&& cb);
        std::exception_ptr handle_query_error(const couchbase::operations::query_response& resp);
        void wrap_query(const std::string& statement,
                        const transaction_query_options& opts,
                        const std::vector<json_string>& params,
                        const nlohmann::json& txdata,
                        const std::string& hook_point,
                        bool check_expiry,
                        std::function<void(std::exception_ptr, couchbase::operations::query_response)>&& cb);

        void handle_err_from_callback(std::exception_ptr e)
        {
            try {
                throw e;
            } catch (const transaction_operation_failed& ex) {
                txn_log->error("op callback called a txn operation that threw exception {}", ex.what());
                op_list_.decrement_ops();
                // presumably that op called op_completed_with_error already, so
                // don't do anything here but swallow it.
            } catch (const async_operation_conflict& op_ex) {
                // the count isn't changed when this is thrown, so just swallow it and log
                txn_log->error("op callback called a txn operation that threw exception {}", op_ex.what());
            } catch (const query_exception& query_ex) {
                txn_log->warn("op callback called a txn operation that threw (and didn't handle) a query_exception {}", query_ex.what());
                errors_.push_back(transaction_operation_failed(FAIL_OTHER, query_ex.what()).cause(query_ex.cause()));
                op_list_.decrement_ops();
            } catch (const std::exception& e) {
                // if the callback throws something which wasn't handled
                // we just want to handle as a rollback
                txn_log->error("op callback threw exception {}", e.what());
                errors_.push_back(transaction_operation_failed(FAIL_OTHER, e.what()));
                op_list_.decrement_ops();
            } catch (...) {
                // could be something really arbitrary, still...
                txn_log->error("op callback threw unexpected exception");
                errors_.push_back(transaction_operation_failed(FAIL_OTHER, "unexpected error"));
                op_list_.decrement_ops();
            }
        }
        template<typename Cb, typename T>
        void op_completed_with_callback(Cb&& cb, std::optional<T> t)
        {
            try {
                op_list_.decrement_in_flight();
                cb({}, t);
                op_list_.decrement_ops();
            } catch (...) {
                handle_err_from_callback(std::current_exception());
            }
        }

        template<typename Cb>
        void op_completed_with_callback(Cb&& cb)
        {
            try {
                op_list_.decrement_in_flight();
                cb({});
                op_list_.decrement_ops();
            } catch (...) {
                handle_err_from_callback(std::current_exception());
            }
        }

        template<typename E>
        void op_completed_with_error(std::function<void(std::exception_ptr)> cb, E err)
        {
            return op_completed_with_error(std::move(cb), std::make_exception_ptr(err));
        }

        void op_completed_with_error(std::function<void(std::exception_ptr)> cb, std::exception_ptr err)
        {
            try {
                std::rethrow_exception(err);
            } catch (const transaction_operation_failed& e) {
                // if this is a transaction_operation_failed, we need to cache it before moving on...
                errors_.push_back(e);
                try {
                    op_list_.decrement_in_flight();
                    cb(std::current_exception());
                    op_list_.decrement_ops();
                } catch (...) {
                    handle_err_from_callback(std::current_exception());
                }
            } catch (...) {
                try {
                    op_list_.decrement_in_flight();
                    cb(std::current_exception());
                    op_list_.decrement_ops();
                } catch (...) {
                    handle_err_from_callback(std::current_exception());
                }
            }
        }

        template<typename Ret, typename E>
        void op_completed_with_error(std::function<void(std::exception_ptr, std::optional<Ret>)> cb, E err)
        {
            return op_completed_with_error(std::move(cb), std::make_exception_ptr(err));
        }

        template<typename Ret>
        void op_completed_with_error(std::function<void(std::exception_ptr, std::optional<Ret>)> cb, std::exception_ptr err)
        {
            try {
                std::rethrow_exception(err);
            } catch (const transaction_operation_failed& e) {
                // if this is a transaction_operation_failed, we need to cache it before moving on...
                errors_.push_back(e);
                try {
                    op_list_.decrement_in_flight();
                    cb(std::current_exception(), std::optional<Ret>());
                    op_list_.decrement_ops();
                } catch (...) {
                    handle_err_from_callback(std::current_exception());
                }
            } catch (...) {
                try {
                    op_list_.decrement_in_flight();
                    cb(std::current_exception(), std::optional<Ret>());
                    op_list_.decrement_ops();
                } catch (...) {
                    handle_err_from_callback(std::current_exception());
                }
            }
        }
        template<typename Ret>
        void op_completed_with_error_no_cache(std::function<void(std::exception_ptr, std::optional<Ret>)> cb, std::exception_ptr err)
        {
            try {
                cb(err, std::optional<Ret>());
            } catch (...) {
                // eat it.
            }
        }

        void op_completed_with_error_no_cache(std::function<void(std::exception_ptr)> cb, std::exception_ptr err)
        {
            try {
                cb(err);
            } catch (...) {
                // just eat it.
            }
        }

        template<typename Handler>
        void cache_error_async(Handler&& cb, std::function<void()> func)
        {
            try {
                op_list_.increment_ops();
                existing_error();
                return func();
            } catch (const async_operation_conflict& e) {
                // can't do anything here but log and eat it.
                error("Attempted to perform txn operation after commit/rollback started");
                // you cannot call op_completed_with_error, as it tries to decrement
                // the op count, however it didn't successfully increment it, so...
                op_completed_with_error_no_cache(cb, std::current_exception());
            } catch (const transaction_operation_failed& e) {
                // thrown only from call_func when previous error exists, so eat it, unless
                // it has PREVIOUS_OP_FAILED cause
                if (e.cause() == PREVIOUS_OPERATION_FAILED) {
                    op_completed_with_error(cb, e);
                }
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

        virtual void remove(const transaction_get_result& document);
        virtual void remove(const transaction_get_result& document, VoidCallback&& cb);

        virtual void query(const std::string& statement, const transaction_query_options& opts, QueryCallback&& cb);
        virtual operations::query_response query(const std::string& statement, const transaction_query_options& opts);

        virtual void commit();
        virtual void commit(VoidCallback&& cb);
        virtual void rollback();
        virtual void rollback(VoidCallback&& cb);

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
        std::atomic<bool> expiry_overtime_mode_{ false };

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

        void atr_commit(bool ambiguity_resolution_mode);

        void atr_commit_ambiguity_resolution();

        void atr_complete();

        void atr_abort();

        void atr_rollback_complete();

        void select_atr_if_needed_unlocked(const couchbase::document_id& id,
                                           std::function<void(std::optional<transaction_operation_failed>)>&& cb);

        template<typename Handler>
        void do_get(const couchbase::document_id& id, const std::optional<std::string> resolving_missing_atr_entry, Handler&& cb);

        void get_doc(
          const couchbase::document_id& id,
          std::function<void(std::optional<error_class>, std::optional<std::string>, std::optional<transaction_get_result>)>&& cb);

        couchbase::operations::mutate_in_request create_staging_request(const couchbase::document_id& in,
                                                                        const transaction_get_result* document,
                                                                        const std::string type,
                                                                        std::optional<std::string> content = std::nullopt);

        template<typename Handler, typename Delay>
        void create_staged_insert(const couchbase::document_id& id, const std::string& content, uint64_t cas, Delay&& delay, Handler&& cb);

        template<typename Handler>
        void create_staged_replace(const transaction_get_result& document, const std::string& content, Handler&& cb);

        template<typename Handler, typename Delay>
        void create_staged_insert_error_handler(const couchbase::document_id& id,
                                                const std::string& content,
                                                uint64_t cas,
                                                Delay&& delay,
                                                Handler&& cb,
                                                error_class ec,
                                                const std::string& message);
    };
} // namespace transactions
} // namespace couchbase
