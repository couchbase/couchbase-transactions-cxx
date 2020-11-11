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

#include <mutex>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <couchbase/client/collection.hxx>
#include <couchbase/transactions/atr_cleanup_entry.hxx>
#include <couchbase/transactions/attempt_context_testing_hooks.hxx>
#include <couchbase/transactions/attempt_state.hxx>
#include <couchbase/transactions/exceptions.hxx>
#include <couchbase/transactions/staged_mutation.hxx>
#include <couchbase/transactions/transaction_config.hxx>
#include <couchbase/transactions/transaction_context.hxx>
#include <couchbase/transactions/transaction_document.hxx>

namespace couchbase
{
namespace transactions
{
    class transactions;
    /**
     * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents, as well as commit or
     * rollback the transaction.
     */
    class attempt_context
    {
      private:
        transactions* parent_;
        transaction_context& overall_;
        const transaction_config& config_;
        boost::optional<std::string> atr_id_;
        std::shared_ptr<collection> atr_collection_;
        bool is_done_;
        staged_mutation_queue staged_mutations_;
        attempt_context_testing_hooks hooks_;
        std::chrono::nanoseconds start_time_server_{ 0 };
        std::vector<transaction_operation_failed> errors_;
        // commit needs to access the hooks
        friend class staged_mutation_queue;
        // entry needs access to private members
        friend class atr_cleanup_entry;

        transaction_document insert_raw(std::shared_ptr<collection> collection, const std::string& id, const nlohmann::json& content);

        transaction_document replace_raw(std::shared_ptr<collection> collection,
                                         const transaction_document& document,
                                         const nlohmann::json& content);

        template<typename R>
        R retry_op(std::function<R()> func)
        {
            do {
                try {
                    return func();
                } catch (const retry_operation& e) {
                    overall_.retry_delay(config_);
                }
            } while (true);
            assert(false && "retry should never reach here");
        }

      public:
        attempt_context(transactions* parent, transaction_context& transaction_ctx, const transaction_config& config);

        /**
         * Gets a document from the specified Couchbase collection matching the specified id.
         *
         * @param collection the Couchbase collection the document exists on
         * @param id the document's ID
         * @return an TransactionDocument containing the document
         */
        // TODO: this should return TransactionGetResult
        transaction_document get(std::shared_ptr<collection> collection, const std::string& id);

        /**
         * Gets a document from the specified Couchbase collection matching the specified id.
         *
         * @param collection the Couchbase collection the document exists on
         * @param id the document's ID
         * @return a TransactionDocument containing the document, if it exists.
         */
        boost::optional<transaction_document> get_optional(std::shared_ptr<collection> collection, const std::string& id);

        /**
         * Mutates the specified document with new content, using the document's last TransactionDocument#cas().
         *
         * The mutation is staged until the transaction is committed.  That is, any read of the document by any Couchbase component will see
         * the document's current value, rather than this staged or 'dirty' data.  If the attempt is rolled back, the staged mutation will
         * be removed.
         *
         * This staged data effectively locks the document from other transactional writes until the attempt completes (commits or rolls
         * back).
         *
         * If the mutation fails, the transaction will automatically rollback this attempt, then retry.
         *
         * @param document the doc to be updated
         * @param content the content to replace the doc with.
         * @return the document, updated with its new CAS value.
         */
        // TODO: this should return a TransactionGetResult
        template<typename Content>
        transaction_document replace(std::shared_ptr<collection> collection, const transaction_document& document, const Content& content)
        {
            nlohmann::json json_content = content;
            return retry_op<transaction_document>(
              [&]() -> transaction_document { return replace_raw(collection, document, json_content); });
        }

        /**
         * Inserts a new document into the specified Couchbase collection.
         *
         * As with #replace, the insert is staged until the transaction is committed.  Due to technical limitations it is not as possible to
         * completely hide the staged data from the rest of the Couchbase platform, as an empty document must be created.
         *
         * This staged data effectively locks the document from other transactional writes until the attempt completes
         * (commits or rolls back).
         *
         * @param collection the Couchbase collection in which to insert the doc
         * @param id the document's unique ID
         * @param content the content to insert
         * @return the doc, updated with its new CAS value and ID, and converted to a TransactionDocument
         */
        // TODO: this should return a TransactionGetResult
        template<typename Content>
        transaction_document insert(std::shared_ptr<collection> collection, const std::string& id, const Content& content)
        {
            nlohmann::json json_content = content;
            return retry_op<transaction_document>([&]() -> transaction_document { return insert_raw(collection, id, json_content); });
        }
        /**
         * Removes the specified document, using the document's last TransactionDocument#cas
         *
         * As with {@link #replace}, the remove is staged until the transaction is committed.  That is, the document will continue to exist,
         * and the rest of the Couchbase platform will continue to see it.
         *
         * This staged data effectively locks the document from other transactional writes until the attempt completes (commits or rolls
         * back).
         *
         * @param document the document to be removed
         */
        void remove(std::shared_ptr<couchbase::collection> collection, transaction_document& document);

        /**
         * Commits the transaction.  All staged replaces, inserts and removals will be written.
         *
         * After this, no further operations are permitted on this instance, and they will result in an
         * exception that will, if not caught in the transaction logic, cause the transaction to
         * fail.
         */
        void commit();

        /**
         * Rollback the transaction.  All staged mutations will be unstaged.
         *
         * Typically, this is called internally to rollback transaction when errors occur in the lambda.  Though
         * it can be called explicitly from the app logic within the transaction as well, perhaps that is better
         * modeled as a custom exception that you raise instead.
         */
        void rollback();

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

        void add_mutation_token()
        {
            overall_.current_attempt().add_mutation_token();
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
        static couchbase::durability_level durability(const transaction_config& config)
        {
            switch (config.durability_level()) {
                case durability_level::NONE:
                    return couchbase::durability_level::none;
                case durability_level::MAJORITY:
                    return couchbase::durability_level::majority;
                case durability_level::MAJORITY_AND_PERSIST_TO_ACTIVE:
                    return couchbase::durability_level::majority_and_persist_to_active;
                case durability_level::PERSIST_TO_MAJORITY:
                    return couchbase::durability_level::persist_to_majority;
            }
            throw std::runtime_error("unknown durability");
        }

        bool expiry_overtime_mode_{ false };

        void check_expiry_pre_commit(std::string stage, boost::optional<const std::string> doc_id);

        // The timing of this call is important.
        // Should be done before doOnNext, which tests often make throw an exception.
        // In fact, needs to be done without relying on any onNext signal.  What if the operation times out instead.
        void check_expiry_during_commit_or_rollback(const std::string& stage, boost::optional<const std::string> doc_id);

        void insure_atr_exists(std::shared_ptr<collection> collection);

        void set_atr_pending_if_first_mutation(std::shared_ptr<collection> collection);

        void error_if_expired_and_not_in_overtime(const std::string& stage, boost::optional<const std::string> doc_id);

        staged_mutation* check_for_own_write(std::shared_ptr<collection> collection, const std::string& id);

        void check_atr_entry_for_blocking_document(const transaction_document& doc);

        /**
         * Don't get blocked by lost transactions (see [BLOCKING] in the RFC)
         *
         * @param doc
         */
        void check_and_handle_blocking_transactions(const transaction_document& doc);

        void check_if_done();

        void atr_commit();

        void atr_commit_ambiguity_resolution();

        void atr_complete();

        void atr_abort();

        void atr_rollback_complete();

        void wrap_collection_call(result& res, std::function<void(result&)> call);

        void select_atr_if_needed(std::shared_ptr<collection> collection, const std::string& id);

        // TODO: this should return boost::optional::<TransactionGetResult>
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
