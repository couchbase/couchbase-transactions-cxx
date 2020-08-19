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
#include <utility>
#include <vector>

#include <couchbase/client/collection.hxx>
#include <couchbase/client/exceptions.hxx>
#include <couchbase/transactions/attempt_context_testing_hooks.hxx>
#include <couchbase/transactions/attempt_state.hxx>
#include <couchbase/transactions/exceptions.hxx>
#include <couchbase/transactions/logging.hxx>
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

      public:
        attempt_context(transactions* parent, transaction_context& transaction_ctx, const transaction_config& config)
          : parent_(parent)
          , overall_(transaction_ctx)
          , config_(config)
          , atr_collection_(nullptr)
          , is_done_(false)
          , hooks_(config.attempt_context_hooks())
        {
            // put a new transaction_attempt in the context...
            overall_.add_attempt();
            spdlog::trace("added new attempt id {} state {}", attempt_id(), attempt_state());
        }

        /**
         * Gets a document from the specified Couchbase collection matching the specified id.
         *
         * @param collection the Couchbase collection the document exists on
         * @param id the document's ID
         * @return an TransactionDocument containing the document
         */
        // TODO: this should return TransactionGetResult
        transaction_document get(std::shared_ptr<collection> collection, const std::string& id)
        {
            auto result = get_optional(collection, id);
            if (result) {
                return result.get();
            }
            spdlog::error("Document with id {} not found", id);
            // TODO: revisit this when re-working exceptions
            throw couchbase::document_not_found_error("Document not found");
        }

        // TODO: this should return boost::optional::<TransactionGetResult>
        boost::optional<transaction_document> get_optional(std::shared_ptr<collection> collection, const std::string& id)
        {
            check_if_done();
            check_expiry_pre_commit(STAGE_GET, id);

            staged_mutation* own_write = check_for_own_write(collection, id);
            if (own_write) {
                spdlog::info("found own-write of mutated doc {}", id);
                return transaction_document::create_from(
                  own_write->doc(), own_write->content<const nlohmann::json&>(), transaction_document_status::OWN_WRITE);
            }
            staged_mutation* own_remove = staged_mutations_.find_remove(collection, id);
            if (own_remove) {
                spdlog::info("found own-write of removed doc {}", id);
                return {};
            }

            hooks_.before_doc_get(this, id);
            const result& res = collection->lookup_in(id,
                                                      { lookup_in_spec::get(ATR_ID).xattr(),
                                                        lookup_in_spec::get(TRANSACTION_ID).xattr(),
                                                        lookup_in_spec::get(ATTEMPT_ID).xattr(),
                                                        lookup_in_spec::get(STAGED_DATA).xattr(),
                                                        lookup_in_spec::get(ATR_BUCKET_NAME).xattr(),
                                                        lookup_in_spec::get(ATR_COLL_NAME).xattr(),
                                                        // For {BACKUP_FIELDS}
                                                        lookup_in_spec::get(TRANSACTION_RESTORE_PREFIX_ONLY).xattr(),
                                                        lookup_in_spec::get(TYPE).xattr(),
                                                        lookup_in_spec::get("$document").xattr(),
                                                        lookup_in_spec::fulldoc_get() });
            if (res.is_not_found()) {
                return {};
            } else if (res.is_success()) {
                transaction_document doc = transaction_document::create_from(*collection, id, res, transaction_document_status::NORMAL);

                if (doc.links().is_document_in_transaction()) {
                    boost::optional<active_transaction_record> atr =
                      active_transaction_record::get_atr(collection, doc.links().atr_id().value(), config_);
                    if (atr) {
                        active_transaction_record& atr_doc = atr.value();
                        boost::optional<atr_entry> entry;
                        for (auto& e : atr_doc.entries()) {
                            if (doc.links().staged_attempt_id().value() == e.attempt_id()) {
                                entry.emplace(e);
                                break;
                            }
                        }
                        bool ignore_doc = false;
                        auto content = doc.content<nlohmann::json>();
                        auto status = doc.status();
                        if (entry) {
                            if (doc.links().staged_attempt_id() && entry->attempt_id() == attempt_id()) {
                                // Attempt is reading its own writes
                                // This is here as backup, it should be returned from the in-memory cache instead
                                content = doc.links().staged_content<nlohmann::json>();
                                status = transaction_document_status::OWN_WRITE;
                            } else {
                                switch (entry->state()) {
                                    case couchbase::transactions::attempt_state::COMMITTED:
                                        if (doc.links().is_document_being_removed()) {
                                            ignore_doc = true;
                                        } else {
                                            content = doc.links().staged_content<nlohmann::json>();
                                            status = transaction_document_status::IN_TXN_COMMITTED;
                                        }
                                        break;
                                    default:
                                        status = transaction_document_status::IN_TXN_OTHER;
                                        if (doc.content<nlohmann::json>().empty()) {
                                            // This document is being inserted, so should not be visible yet
                                            ignore_doc = true;
                                        }
                                        break;
                                }
                            }
                        } else {
                            // Don't know if transaction was committed or rolled back. Should not happen as ATR should stick around long
                            // enough
                            status = transaction_document_status::AMBIGUOUS;
                            if (content.empty()) {
                                // This document is being inserted, so should not be visible yet
                                ignore_doc = true;
                            }
                        }
                        if (ignore_doc) {
                            return {};
                        } else {
                            return transaction_document::create_from(doc, content, status);
                        }
                    } else {
                        // failed to get the ATR
                        if (doc.content<nlohmann::json>().empty()) {
                            // this document is being inserted, so should not be visible yet
                            return {};
                        } else {
                            doc.status(transaction_document_status::AMBIGUOUS);
                            return doc;
                        }
                    }
                }
                return doc;
            } else {
                std::string what(fmt::format("got error while getting doc {}: {}", id, res.strerror()));
                spdlog::warn(what);
                throw error_wrapper(FAIL_OTHER, what);
            }
        }

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
            check_if_done();
            check_expiry_pre_commit(STAGE_REPLACE, document.id());
            check_and_handle_blocking_transactions(document);
            select_atr_if_needed(collection, document.id());
            set_atr_pending_if_first_mutation(collection);

            hooks_.before_staged_replace(this, document.id());
            spdlog::trace("about to replace doc {} with cas {} in txn {}", document.id(), document.cas(), overall_.transaction_id());
            std::vector<mutate_in_spec> specs = {
                mutate_in_spec::upsert(TRANSACTION_ID, overall_.transaction_id()).xattr().create_path(),
                mutate_in_spec::upsert(ATTEMPT_ID, attempt_id()).xattr(),
                mutate_in_spec::upsert(ATR_ID, atr_id_.value()).xattr(),
                mutate_in_spec::upsert(ATR_BUCKET_NAME, collection->bucket_name()).xattr(),
                mutate_in_spec::upsert(ATR_COLL_NAME, collection->scope() + "." + collection->name()).xattr(),
                mutate_in_spec::upsert(TYPE, "replace").xattr(),
            };
            if (document.metadata()) {
                if (document.metadata()->cas()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_CAS, document.metadata()->cas().value()).xattr());
                }
                if (document.metadata()->revid()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_REVID, document.metadata()->revid().value()).xattr());
                }
                if (document.metadata()->exptime()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_EXPTIME, document.metadata()->exptime().value()).xattr());
                }
            }

            const result& res = collection->mutate_in(document.id(), specs, durability(config_));

            if (res.is_success()) {
                transaction_document out = document;
                out.cas(res.cas);
                staged_mutations_.add(staged_mutation(out, content, staged_mutation_type::REPLACE));
                add_mutation_token();
                return out;
            }
            throw error_wrapper(FAIL_OTHER, std::string("failed to replace the document: ") + res.strerror());
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
            check_if_done();
            check_expiry_pre_commit(STAGE_INSERT, id);
            select_atr_if_needed(collection, id);
            set_atr_pending_if_first_mutation(collection);

            hooks_.before_staged_insert(this, id);
            spdlog::info("about to insert staged doc {}", id);
            check_expiry_during_commit_or_rollback(STAGE_CREATE_STAGED_INSERT, id);
            const result& res = collection->mutate_in(
              id,
              {
                mutate_in_spec::upsert(TRANSACTION_ID, overall_.transaction_id()).xattr().create_path(),
                mutate_in_spec::insert(ATTEMPT_ID, attempt_id()).xattr(),
                mutate_in_spec::insert(ATR_ID, atr_id_.value()).xattr(),
                mutate_in_spec::insert(STAGED_DATA, content).xattr(),
                mutate_in_spec::insert(ATR_BUCKET_NAME, collection->bucket_name()).xattr(),
                mutate_in_spec::insert(ATR_COLL_NAME, collection->scope() + "." + collection->name()).xattr().create_path(),
                mutate_in_spec::insert(TYPE, "insert").xattr(),
                mutate_in_spec::fulldoc_insert(nlohmann::json::object()),
              },
              durability(config_));
            spdlog::info("inserted doc {} CAS={}, rc={}", id, res.cas, res.strerror());
            hooks_.after_staged_insert_complete(this, id);
            if (res.is_success()) {
                // TODO: clean this up (do most of this in transactions_document(...))
                transaction_links links(atr_id_,
                                        collection->bucket_name(),
                                        collection->scope(),
                                        collection->name(),
                                        overall_.transaction_id(),
                                        attempt_id(),
                                        nlohmann::json(content),
                                        boost::none,
                                        boost::none,
                                        boost::none,
                                        std::string("insert"));
                transaction_document out(id, content, res.cas, *collection, links, transaction_document_status::NORMAL, boost::none);
                staged_mutations_.add(staged_mutation(out, content, staged_mutation_type::INSERT));
                add_mutation_token();
                return out;
            }
            // TODO: RETRY-ERR-AMBIG
            // TODO: handle document already exists
            // For now, anything is a fail.
            throw error_wrapper(FAIL_OTHER, std::string("failed to insert the document: ") + res.strerror());
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
        void remove(std::shared_ptr<couchbase::collection> collection, transaction_document& document)
        {
            check_if_done();
            check_expiry_pre_commit(STAGE_REMOVE, document.id());
            select_atr_if_needed(collection, document.id());
            set_atr_pending_if_first_mutation(collection);

            hooks_.before_staged_remove(this, document.id());
            spdlog::info("about to remove remove doc {} with cas {}", document.id(), document.cas());
            std::vector<mutate_in_spec> specs = {
                mutate_in_spec::upsert(TRANSACTION_ID, overall_.transaction_id()).xattr().create_path(),
                mutate_in_spec::upsert(ATTEMPT_ID, attempt_id()).xattr(),
                mutate_in_spec::upsert(ATR_ID, atr_id_.value()).xattr(),
                mutate_in_spec::upsert(ATR_BUCKET_NAME, collection->bucket_name()).xattr(),
                mutate_in_spec::upsert(ATR_COLL_NAME, collection->scope() + "." + collection->name()).xattr(),
                mutate_in_spec::upsert(TYPE, "remove").xattr(),
            };
            if (document.metadata()) {
                if (document.metadata()->cas()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_CAS, document.metadata()->cas().value()).xattr());
                }
                if (document.metadata()->revid()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_REVID, document.metadata()->revid().value()).xattr());
                }
                if (document.metadata()->exptime()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_EXPTIME, document.metadata()->exptime().value()).xattr());
                }
            }
            specs.emplace_back(mutate_in_spec::upsert(STAGED_DATA, REMOVE_SENTINEL).xattr());
            const result& res = collection->mutate_in(document.id(), specs, durability(config_));
            spdlog::info("removed doc {} CAS={}, rc={}", document.id(), res.cas, res.strerror());
            hooks_.after_staged_remove_complete(this, document.id());
            if (res.is_success()) {
                document.cas(res.cas);
                // TODO: overwriting insert
                staged_mutations_.add(staged_mutation(document, "", staged_mutation_type::REMOVE));
                add_mutation_token();
            } else {
                auto msg = std::string("failed to remove document: ") + res.strerror();
                spdlog::error(msg);
                throw error_wrapper(FAIL_OTHER, msg);
            }
        }

        /**
         * Commits the transaction.  All staged replaces, inserts and removals will be written.
         *
         * After this, no further operations are permitted on this instance, and they will result in an
         * exception that will, if not caught in the transaction logic, cause the transaction to
         * fail.
         */
        void commit()
        {
            spdlog::info("commit {}", attempt_id());
            check_expiry_pre_commit(STAGE_BEFORE_COMMIT, {});
            if (atr_collection_ && atr_id_ && !is_done_) {
                std::string prefix(ATR_FIELD_ATTEMPTS + "." + attempt_id() + ".");
                std::vector<mutate_in_spec> specs({
                  mutate_in_spec::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(couchbase::transactions::attempt_state::COMMITTED))
                    .xattr(),
                  mutate_in_spec::upsert(prefix + ATR_FIELD_START_COMMIT, "${Mutation.CAS}").xattr().expand_macro(),
                });
                hooks_.before_atr_commit(this);
                staged_mutations_.extract_to(prefix, specs);
                const result& res = atr_collection_->mutate_in(atr_id_.value(), specs);
                hooks_.after_atr_commit(this);
                if (res.is_success()) {
                    std::vector<transaction_document> docs;
                    staged_mutations_.commit();
                    // if this succeeds, set ATR to COMPLETED
                    std::string prefix(ATR_FIELD_ATTEMPTS + "." + attempt_id() + ".");
                    std::vector<mutate_in_spec> specs({
                      mutate_in_spec::upsert(prefix + ATR_FIELD_STATUS,
                                             attempt_state_name(couchbase::transactions::attempt_state::COMPLETED))
                        .xattr(),
                      mutate_in_spec::upsert(prefix + ATR_FIELD_TIMESTAMP_COMPLETE, "${Mutation.CAS}").xattr().expand_macro(),
                    });
                    const result& atr_res = atr_collection_->mutate_in(atr_id_.value(), specs);
                    if (atr_res.is_success()) {
                        is_done_ = true;
                        spdlog::trace("setting attempt state COMPLETED for attempt {}", atr_id_.value());
                        attempt_state(couchbase::transactions::attempt_state::COMPLETED);
                    } else {
                        // TODO: recheck this, but I believe this is fine, we should log it and cleanup later
                        spdlog::error("seting COMPLETED on attempt {} for ATR {} failed!", attempt_id(), atr_id_.value());
                    }

                } else {
                    throw error_wrapper(FAIL_OTHER, std::string("failed to commit transaction: ") + attempt_id() + ": " + res.strerror());
                }
            } else {
                // no mutation, no need to commit
                if (!is_done_) {
                    spdlog::info("calling commit on attempt that has got no mutations, skipping");
                    is_done_ = true;
                    return;
                } else {
                    // do not rollback or retry
                    throw error_wrapper(FAIL_OTHER, std::string("calling commit on attempt that is already completed"), false, false);
                }
            }
        }
        /**
         * Rollback the transaction.  All staged mutations will be unstaged.
         *
         * Typically, this is called internally to rollback transaction when errors occur in the lambda.  Though
         * it can be called explicitly from the app logic within the transaction as well, perhaps that is better
         * modeled as a custom exception that you raise instead.
         */
        void rollback()
        {
            spdlog::info("rolling back");
            // check for expiry
            check_expiry_during_commit_or_rollback(STAGE_ROLLBACK, boost::none);
            if (!atr_id_ || !atr_collection_) {
                // TODO: check this, but if we try to rollback an empty txn, we should
                // prevent a subsequent commit
                spdlog::trace("rollback called on txn with no mutations");
                is_done_ = true;
                return;
            }
            if (is_done()) {
                std::string msg("Transaction already done, cannot rollback");
                spdlog::error(msg);
                // need to raise a FAIL_OTHER which is not retryable or rollback-able
                throw error_wrapper(FAIL_OTHER, msg, false, false);
            }
            // We do 3 things - set the atr to abort
            //                - unstage the docs
            //                - set atr to ROLLED_BACK
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + attempt_id() + ".");
            std::vector<mutate_in_spec> specs({
              mutate_in_spec::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(couchbase::transactions::attempt_state::ABORTED))
                .xattr(),
              mutate_in_spec::upsert(prefix + ATR_FIELD_TIMESTAMP_ROLLBACK_START, "${Mutation.CAS}").xattr().expand_macro(),
            });
            // now add the staged mutations...
            staged_mutations_.extract_to(prefix, specs);

            hooks_.before_atr_aborted(this);
            const result& res = atr_collection_->mutate_in(atr_id_.value(), specs);
            hooks_.after_atr_aborted(this);
            spdlog::trace("rollback completed atr abort phase");
            if (res.is_success()) {
                staged_mutations_.iterate([&](staged_mutation& mutation) {
                    hooks_.before_doc_rolled_back(this, mutation.doc().id());
                    std::vector<mutate_in_spec> specs({
                      mutate_in_spec::upsert(TRANSACTION_INTERFACE_PREFIX_ONLY, nullptr).xattr(),
                    });
                    switch (mutation.type()) {
                        case staged_mutation_type::INSERT:
                            spdlog::trace("rolling back staged insert for {}", mutation.doc().id());
                            // TODO: since we don't insert as deleted yet, instead of mutating this, we
                            // need to remove it
                            // mutation.doc().collection_ref().mutate_in(mutation.doc().id(), specs);
                            mutation.doc().collection_ref().remove(mutation.doc().id());
                            hooks_.after_rollback_replace_or_remove(this, mutation.doc().id());
                            // TODO: deal with errors mutating the doc
                            break;
                        default:
                            spdlog::trace("rolling back staged remove/replace for {}", mutation.doc().id());
                            auto r = mutation.doc().collection_ref().mutate_in(mutation.doc().id(), specs);
                            spdlog::trace("rollback result {}", r.to_string());
                            hooks_.after_rollback_replace_or_remove(this, mutation.doc().id());
                            // TODO: deal with errors mutating the doc
                            break;
                    }
                    spdlog::trace("rollback completed unstaging docs");
                });

                // now complete the atr rollback
                hooks_.before_atr_rolled_back(this);
                std::vector<mutate_in_spec> specs({
                  mutate_in_spec::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(couchbase::transactions::attempt_state::ROLLED_BACK))
                    .xattr(),
                  mutate_in_spec::upsert(prefix + ATR_FIELD_TIMESTAMP_ROLLBACK_COMPLETE, "${Mutation.CAS}").xattr().expand_macro(),
                });
                atr_collection_->mutate_in(atr_id_.value(), specs);
                attempt_state(couchbase::transactions::attempt_state::ROLLED_BACK);
                hooks_.after_atr_rolled_back(this);
                is_done_ = true;
                // TODO: deal with errors mutating ATR record, and retries perhaps?
            } else {
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

        CB_NODISCARD const std::string& attempt_id()
        {
            return overall_.current_attempt().id;
        }

        CB_NODISCARD const couchbase::transactions::attempt_state attempt_state()
        {
            return overall_.current_attempt().state;
        }

        void attempt_state(couchbase::transactions::attempt_state s)
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

        bool has_expired_client_side(std::string place, boost::optional<const std::string> doc_id)
        {
            bool over = overall_.has_expired_client_side(config_);
            bool hook = hooks_.has_expired_client_side_hook(this, place, doc_id);
            if (over) {
                spdlog::info("{} expired in {}", attempt_id(), place);
            }
            if (hook) {
                spdlog::info("{} fake expiry in {}", attempt_id(), place);
            }
            return over || hook;
        }

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

        void check_expiry_pre_commit(std::string stage, boost::optional<const std::string> doc_id)
        {
            if (has_expired_client_side(stage, std::move(doc_id))) {
                spdlog::info(
                  "{} has expired in stage {}, entering expiry-overtime mode - will make one attempt to rollback", attempt_id(), stage);

                // [EXP-ROLLBACK] Combo of setting this mode and throwing AttemptExpired will result in a attempt to rollback, which will
                // ignore expiries, and bail out if anything fails
                expiry_overtime_mode_ = true;
                throw attempt_expired(std::string("Attempt has expired in stage ") + stage);
            }
        }

        // The timing of this call is important.
        // Should be done before doOnNext, which tests often make throw an exception.
        // In fact, needs to be done without relying on any onNext signal.  What if the operation times out instead.
        void check_expiry_during_commit_or_rollback(const std::string& stage, boost::optional<const std::string> doc_id)
        {
            // [EXP-COMMIT-OVERTIME]
            if (!expiry_overtime_mode_) {
                if (has_expired_client_side(stage, std::move(doc_id))) {
                    spdlog::info(
                      "{} has expired in stage {}, entering expiry-overtime mode (one attempt to complete commit)", attempt_id(), stage);
                    expiry_overtime_mode_ = true;
                }
            } else {
                spdlog::info("{} ignoring expiry in stage {}  as in expiry-overtime mode", attempt_id(), stage);
            }
        }

        void insure_atr_exists(std::shared_ptr<collection> collection)
        {
            // TODO: use exists here when collection supports it
            result res = collection->get(atr_id_.value());
            // just upsert an empty doc for now
            if (res.is_success()) {
                return;
            }
            collection->upsert(atr_id_.value(), nlohmann::json::object());
        }
        void set_atr_pending_if_first_mutation(std::shared_ptr<collection> collection)
        {
            if (staged_mutations_.empty()) {
                std::string prefix(ATR_FIELD_ATTEMPTS + "." + attempt_id() + ".");
                if (!atr_id_) {
                    throw error_wrapper(FAIL_OTHER, std::string("ATR ID is not initialized"));
                }
                insure_atr_exists(collection);
                hooks_.before_atr_pending(this);
                spdlog::info("updating atr {}", atr_id_.value());
                const result& res = collection->mutate_in(
                  atr_id_.value(),
                  { mutate_in_spec::insert(prefix + ATR_FIELD_TRANSACTION_ID, overall_.transaction_id()).xattr().create_path(),
                    mutate_in_spec::insert(prefix + ATR_FIELD_STATUS, attempt_state_name(couchbase::transactions::attempt_state::PENDING))
                      .xattr()
                      .create_path(),
                    mutate_in_spec::insert(prefix + ATR_FIELD_START_TIMESTAMP, mutate_in_macro::CAS).xattr().expand_macro(),
                    mutate_in_spec::insert(prefix + ATR_FIELD_EXPIRES_AFTER_MSECS,
                                           std::chrono::duration_cast<std::chrono::milliseconds>(config_.expiration_time()).count())
                      .xattr() },
                  durability(config_));
                if (res.is_success()) {
                    spdlog::info("set ATR {}/{}/{} to Pending, got CAS (start time) {}",
                                 collection->bucket_name(),
                                 collection->name(),
                                 atr_id_.value(),
                                 res.cas);
                    start_time_server_ = std::chrono::nanoseconds(res.cas);
                    hooks_.after_atr_pending(this);
                    check_expiry_during_commit_or_rollback(STAGE_ATR_PENDING, {});
                } else if (res.is_value_too_large()) {
                    // TODO: Handle "active transaction record is full" condition
                    throw error_wrapper(FAIL_ATR_FULL, "ATR is full");
                } else {
                    std::string what(fmt::format("got error while setting atr {}: {}", atr_id_.value(), res.strerror()));
                    spdlog::warn(what);
                    throw error_wrapper(FAIL_OTHER, what);
                }
            }
        }

        staged_mutation* check_for_own_write(std::shared_ptr<collection> collection, const std::string& id)
        {
            staged_mutation* own_replace = staged_mutations_.find_replace(collection, id);
            if (own_replace) {
                return own_replace;
            }
            staged_mutation* own_insert = staged_mutations_.find_insert(collection, id);
            if (own_insert) {
                return own_insert;
            }
            return nullptr;
        }

        void check_atr_entry_for_blocking_document(const transaction_document& doc);

        /**
         * Don't get blocked by lost transactions (see [BLOCKING] in the RFC)
         *
         * @param doc
         */
        void check_and_handle_blocking_transactions(const transaction_document& doc)
        {
            // The main reason to require doc to be fetched inside the transaction is so we can detect this on the client side
            if (doc.links().has_staged_write()) {
                // Check not just writing the same doc twice in the same transaction
                // NOTE: we check the transaction rather than attempt id. This is to handle [RETRY-ERR-AMBIG-REPLACE].
                if (doc.links().staged_transaction_id().value() == overall_.transaction_id()) {
                    spdlog::info("doc {} has been written by this transaction, ok to continue", doc.id());
                } else {
                    // Note that it is essential not to get blocked permanently here. Blocker could be a crashed pending transaction, in
                    // which case the lost cleanup thread cannot do anything (it does not know which docs are in a pending transaction).
                    auto elapsed_since_start_of_txn = std::chrono::duration_cast<std::chrono::milliseconds>(
                      std::chrono::system_clock::now() - overall_.start_time_client());
                    if (elapsed_since_start_of_txn.count() < 1000) {
                        spdlog::info("doc {} is in another transaction {}, just retrying as only {}ms since start",
                                     doc.id(),
                                     doc.links().staged_attempt_id().get(),
                                     elapsed_since_start_of_txn.count());
                        throw document_already_in_transaction(std::string("TODO"));
                    } else {
                        // The blocking transaction has been blocking for a suspiciosly long time. Time to start checking if it is expired
                        if (doc.links().atr_id() && doc.links().atr_bucket_name()) {
                            spdlog::info("doc {} is in another transaction {}, after {}ms since start, so checking ATR entry {}/{}/{}",
                                         doc.id(),
                                         doc.links().staged_attempt_id().get(),
                                         elapsed_since_start_of_txn.count(),
                                         doc.links().atr_bucket_name().value(),
                                         doc.links().atr_collection_name().value_or(""),
                                         doc.links().atr_id().value());
                            check_atr_entry_for_blocking_document(doc);
                        } else {
                            spdlog::info("doc {} is in another transaction {}, after {}ms but cannot check ATR - probablu a bug, so "
                                         "proceeding to overwrite",
                                         doc.id(),
                                         doc.links().staged_attempt_id().get(),
                                         elapsed_since_start_of_txn.count());
                        }
                    }
                }
            }
        }

        void check_if_done()
        {
            if (is_done_) {
                throw error_wrapper(
                  FAIL_OTHER, "Cannot perform operations after transaction has been committed or rolled back", false, false);
            }
        }

        void select_atr_if_needed(std::shared_ptr<collection> collection, const std::string& id);
    };
} // namespace transactions
} // namespace couchbase
