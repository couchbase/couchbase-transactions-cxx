#pragma once

#include <chrono>
#include <list>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#include <couchbase/client/collection.hxx>
#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/attempt_state.hxx>
#include <couchbase/transactions/transaction_document.hxx>

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
        boost::optional<std::string> atr_id_;
        std::shared_ptr<collection> atr_collection_;
        bool is_done_;
        std::unique_ptr<staged_mutation_queue> staged_mutations_;
        attempt_context_testing_hooks hooks_;
        std::list<transaction_operation_failed> errors_;
        // commit needs to access the hooks
        friend class staged_mutation_queue;
        // entry needs access to private members
        friend class atr_cleanup_entry;

        virtual transaction_document insert_raw(std::shared_ptr<collection> collection,
                                                const std::string& id,
                                                const nlohmann::json& content);

        virtual transaction_document replace_raw(std::shared_ptr<collection> collection,
                                                 const transaction_document& document,
                                                 const nlohmann::json& content);

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

      public:
        attempt_context_impl(transactions* parent, transaction_context& transaction_ctx, const transaction_config& config);
        ~attempt_context_impl();

        virtual transaction_document get(std::shared_ptr<collection> collection, const std::string& id);
        virtual boost::optional<transaction_document> get_optional(std::shared_ptr<collection> collection, const std::string& id);
        virtual void remove(std::shared_ptr<couchbase::collection> collection, transaction_document& document);
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

        CB_NODISCARD const attempt_state state()
        {
            return overall_.current_attempt().state;
        }

        void state(couchbase::transactions::attempt_state s)
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

        bool has_expired_client_side(std::string place, boost::optional<const std::string> doc_id);

      private:
        bool expiry_overtime_mode_{ false };

        void check_expiry_pre_commit(std::string stage, boost::optional<const std::string> doc_id);

        void check_expiry_during_commit_or_rollback(const std::string& stage, boost::optional<const std::string> doc_id);

        void set_atr_pending_if_first_mutation(std::shared_ptr<collection> collection);

        void error_if_expired_and_not_in_overtime(const std::string& stage, boost::optional<const std::string> doc_id);

        staged_mutation* check_for_own_write(std::shared_ptr<collection> collection, const std::string& id);

        void check_and_handle_blocking_transactions(const transaction_document& doc, forward_compat_stage stage);

        void check_atr_entry_for_blocking_document(const transaction_document& doc);

        void check_if_done();

        void atr_commit();

        void atr_commit_ambiguity_resolution();

        void atr_complete();

        void atr_abort();

        void atr_rollback_complete();

        void select_atr_if_needed(std::shared_ptr<collection> collection, const std::string& id);

        boost::optional<transaction_document> do_get(std::shared_ptr<collection> collection, const std::string& id);

        boost::optional<std::pair<transaction_document, couchbase::result>> get_doc(std::shared_ptr<couchbase::collection> collection,
                                                                                    const std::string& id);

        transaction_document create_staged_insert(std::shared_ptr<collection> collection,
                                                  const std::string& id,
                                                  const nlohmann::json& content,
                                                  uint64_t& cas);
    };
} // namespace transactions
} // namespace couchbase
