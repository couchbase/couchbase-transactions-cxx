#pragma once

#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <boost/core/ignore_unused.hpp>

#include <couchbase/client/collection.hxx>
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
        attempt_state state_;
        std::string attempt_id_;
        staged_mutation_queue staged_mutations_;
        attempt_context_testing_hooks hooks_;
        std::chrono::nanoseconds start_time_server_{ 0 };

      public:
        attempt_context(transactions* parent,
                        transaction_context& transaction_ctx,
                        const transaction_config& config)
          : parent_(parent)
          , overall_(transaction_ctx)
          , config_(config)
          , state_(attempt_state::NOT_STARTED)
          , attempt_id_(uid_generator::next())
          , atr_collection_(nullptr)
          , is_done_(false)
          , hooks_(config.attempt_context_hooks())
        {
        }

        /**
         * Gets a document from the specified Couchbase collection matching the specified id.
         *
         * @param collection the Couchbase collection the document exists on
         * @param id the document's ID
         * @return an TransactionDocument containing the document
         */
        boost::optional<transaction_document> get(std::shared_ptr<collection> collection, const std::string& id)
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
                    if (atr.has_value()) {
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
                        if (entry.has_value()) {
                            if (doc.links().staged_attempt_id().has_value() && entry->attempt_id() == attempt_id_) {
                                // Attempt is reading its own writes
                                // This is here as backup, it should be returned from the in-memory cache instead
                                content = doc.links().staged_content<nlohmann::json>();
                                status = transaction_document_status::OWN_WRITE;
                            } else {
                                switch (entry->state()) {
                                    case attempt_state::COMMITTED:
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
                throw client_error(what);
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
        template<typename Content>
        transaction_document replace(std::shared_ptr<collection> collection, const transaction_document& document, const Content& content)
        {
            check_if_done();
            check_expiry_pre_commit(STAGE_REPLACE, document.id());
            check_and_handle_blocking_transactions(document);
            select_atr_if_needed(collection, document.id());
            set_atr_pending_if_first_mutation(collection);

            hooks_.before_staged_replace(this, document.id());
            spdlog::trace("about to replace doc {} with cas {}", document.id(), document.cas());
            std::vector<mutate_in_spec> specs = {
                mutate_in_spec::upsert(TRANSACTION_ID, overall_.transaction_id()).xattr().create_path(),
                mutate_in_spec::upsert(ATTEMPT_ID, attempt_id_).xattr(),
                mutate_in_spec::upsert(ATR_ID, atr_id_.value()).xattr(),
                mutate_in_spec::upsert(ATR_BUCKET_NAME, collection->bucket_name()).xattr(),
                mutate_in_spec::upsert(ATR_COLL_NAME, collection->scope() + "." + collection->name()).xattr(),
                mutate_in_spec::upsert(TYPE, "replace").xattr(),
            };
            if (document.metadata().has_value()) {
                if (document.metadata()->cas().has_value()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_CAS, document.metadata()->cas().value()));
                }
                if (document.metadata()->revid().has_value()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_CAS, document.metadata()->revid().value()));
                }
                if (document.metadata()->exptime().has_value()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_CAS, document.metadata()->exptime().value()));
                }
            }
            specs.emplace_back(mutate_in_spec::fulldoc_upsert(content));
            const result& res = collection->mutate_in(document.id(), specs, durability(config_));

            if (res.is_success()) {
                transaction_document out = document;
                out.cas(res.cas);
                staged_mutations_.add(staged_mutation(out, content, staged_mutation_type::REPLACE));
                return out;
            }
            throw std::runtime_error(std::string("failed to replace the document: ") + res.strerror());
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
                mutate_in_spec::insert(ATTEMPT_ID, attempt_id_).xattr(),
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
                transaction_document out = transaction_document::create_from(*collection, id, res, transaction_document_status::NORMAL);
                staged_mutations_.add(staged_mutation(out, content, staged_mutation_type::INSERT));
                return out;
            }
            // TODO: RETRY-ERR-AMBIG
            // TODO: handle document already exists
            throw client_error(std::string("failed to insert the document: ") + res.strerror());
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
                mutate_in_spec::upsert(ATTEMPT_ID, attempt_id_).xattr(),
                mutate_in_spec::upsert(ATR_ID, atr_id_.value()).xattr(),
                mutate_in_spec::upsert(ATR_BUCKET_NAME, collection->bucket_name()).xattr(),
                mutate_in_spec::upsert(ATR_COLL_NAME, collection->scope() + "." + collection->name()).xattr(),
                mutate_in_spec::upsert(TYPE, "remove").xattr(),
            };
            if (document.metadata().has_value()) {
                if (document.metadata()->cas().has_value()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_CAS, document.metadata()->cas().value()));
                }
                if (document.metadata()->revid().has_value()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_CAS, document.metadata()->revid().value()));
                }
                if (document.metadata()->exptime().has_value()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_CAS, document.metadata()->exptime().value()));
                }
            }
            specs.emplace_back(mutate_in_spec::upsert(STAGED_DATA, REMOVE_SENTINEL));
            const result& res = collection->mutate_in(document.id(), specs, durability(config_));
            spdlog::info("removed doc {} CAS={}, rc={}", document.id(), res.cas, res.strerror());
            hooks_.after_staged_remove_complete(this, document.id());
            if (res.is_success()) {
                document.cas(res.cas);
                // TODO: overwriting insert
                staged_mutations_.add(staged_mutation(document, "", staged_mutation_type::REMOVE));
                return;
            }
            throw std::runtime_error(std::string("failed to remove the document: ") + res.strerror());
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
            spdlog::info("commit {}", attempt_id_);
            check_expiry_pre_commit(STAGE_BEFORE_COMMIT, {});
            if (atr_collection_ && atr_id_.has_value() && !is_done_) {
                std::string prefix(ATR_FIELD_ATTEMPTS + "." + attempt_id_ + ".");
                std::vector<mutate_in_spec> specs({
                  mutate_in_spec::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::COMMITTED)).xattr(),
                  mutate_in_spec::upsert(prefix + ATR_FIELD_START_COMMIT, "${Mutation.CAS}").xattr().expand_macro(),
                });
                staged_mutations_.extract_to(prefix, specs);
                const result& res = atr_collection_->mutate_in(atr_id_.value(), specs);
                if (res.is_success()) {
                    std::vector<transaction_document> docs;
                    staged_mutations_.commit();
                    is_done_ = true;
                    state_ = attempt_state::COMMITTED;
                } else {
                    throw std::runtime_error(std::string("failed to commit transaction: ") + attempt_id_ + ": " + res.strerror());
                }
            } else {
                // no mutation, no need to commit
                if (!is_done_) {
                    spdlog::info("calling commit on attempt that has got no mutations, skipping");
                    is_done_ = true;
                    state_ = attempt_state::COMPLETED;
                    return;
                } else {
                    throw attempt_exception("calling commit on attempt that is already completed");
                }
            }
        }

        [[nodiscard]] bool is_done()
        {
            return is_done_;
        }

        [[nodiscard]] const std::string& id()
        {
            return attempt_id_;
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

        void check_expiry_pre_commit(std::string stage, boost::optional<std::string> doc_id)
        {
            if (has_expired_client_side(stage, std::move(doc_id))) {
                spdlog::info(
                  "{} has expired in stage {}, entering expiry-overtime mode - will make one attempt to rollback", attempt_id_, stage);

                // [EXP-ROLLBACK] Combo of setting this mode and throwing AttemptExpired will result in a attempt to rollback, which will
                // ignore expiries, and bail out if anything fails
                expiry_overtime_mode_ = true;
                throw attempt_expired("Attempt has expired in stage " + stage);
            }
        }

        // The timing of this call is important.
        // Should be done before doOnNext, which tests often make throw an exception.
        // In fact, needs to be done without relying on any onNext signal.  What if the operation times out instead.
        void check_expiry_during_commit_or_rollback(const std::string& stage, boost::optional<std::string> doc_id)
        {
            // [EXP-COMMIT-OVERTIME]
            if (!expiry_overtime_mode_) {
                if (has_expired_client_side(stage, std::move(doc_id))) {
                    spdlog::info(
                      "{} has expired in stage {}, entering expiry-overtime mode (one attempt to complete commit)", attempt_id_, stage);
                    expiry_overtime_mode_ = true;
                }
            } else {
                spdlog::info("{} ignoring expiry in stage {}  as in expiry-overtime mode", attempt_id_, stage);
            }
        }

        void insure_atr_exists(std::shared_ptr<collection> collection) {
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
                std::string prefix(ATR_FIELD_ATTEMPTS + "." + attempt_id_ + ".");
                if (!atr_id_.has_value()) {
                    throw std::domain_error("ATR ID is not initialized");
                }
                insure_atr_exists(collection);
                hooks_.before_atr_pending(this);
                spdlog::info("updating atr {}", atr_id_.value());
                const result& res = collection->mutate_in(
                  atr_id_.value(),
                  {
                    mutate_in_spec::insert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::PENDING)).xattr().create_path(),
                    mutate_in_spec::insert(prefix + ATR_FIELD_START_TIMESTAMP, mutate_in_macro::CAS).xattr().expand_macro(),
                    mutate_in_spec::insert(prefix + ATR_FIELD_EXPIRES_AFTER_MSECS,
                                           std::chrono::duration_cast<std::chrono::milliseconds>(config_.expiration_time()).count())
                      .xattr()
                  },
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
                } else {
                    std::string what(fmt::format("got error while setting atr {}: {}", atr_id_.value(), res.strerror()));
                    spdlog::warn(what);
                    throw client_error(what);
                }
            }
        }

        bool has_expired_client_side(std::string place, boost::optional<std::string> doc_id)
        {
            bool over = overall_.has_expired_client_side(config_);
            bool hook = hooks_.has_expired_client_side_hook(this, place, doc_id);
            if (over) {
                spdlog::info("{} expired in {}", attempt_id_, place);
            }
            if (hook) {
                spdlog::info("{} fake expiry in {}", attempt_id_, place);
            }
            return over || hook;
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
                        throw document_already_in_transaction("TODO");
                    } else {
                        // The blocking transaction has been blocking for a suspiciosly long time. Time to start checking if it is expired
                        if (doc.links().atr_id().has_value() && doc.links().atr_bucket_name().has_value()) {
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
                throw std::domain_error("Cannot perform operations after transaction has been committed or rolled back");
            }
        }

        void select_atr_if_needed(std::shared_ptr<collection> collection, const std::string& id);
    };
} // namespace transactions
} // namespace couchbase
