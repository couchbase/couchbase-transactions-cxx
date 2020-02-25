#pragma once

#include <mutex>
#include <string>
#include <vector>

#include <couchbase/client/collection.hxx>
#include <couchbase/transactions/attempt_state.hxx>
#include <couchbase/transactions/configuration.hxx>
#include <couchbase/transactions/logging.hxx>
#include <couchbase/transactions/staged_mutation.hxx>
#include <couchbase/transactions/transaction_context.hxx>
#include <couchbase/transactions/transaction_document.hxx>

namespace couchbase
{
namespace transactions
{
    /**
     * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents, as well as commit or
     * rollback the transaction.
     */
    class attempt_context
    {
      private:
        transaction_context& txctx_;
        const configuration& config_;
        boost::optional<std::string> atr_id_;
        collection* atr_collection_;
        bool is_done_;
        attempt_state state_;
        std::string id_;
        staged_mutation_queue staged_mutations_;

        void init_atr_if_needed(collection* collection, const std::string& id);

      public:
        attempt_context(transaction_context& transaction_ctx, const configuration& config)
          : txctx_(transaction_ctx)
          , config_(config)
          , state_(attempt_state::NOT_STARTED)
          , id_(uid_generator::next())
          , atr_collection_(nullptr)
          , is_done_(false)
        {
        }

        /**
         * Gets a document from the specified Couchbase collection matching the specified id.
         *
         * @param collection the Couchbase collection the document exists on
         * @param id the document's ID
         * @return an TransactionDocument containing the document
         */
        boost::optional<transaction_document> get(collection* collection, const std::string& id)
        {
            staged_mutation* mutation = staged_mutations_.find_replace(collection, id);
            if (mutation == nullptr) {
                mutation = staged_mutations_.find_insert(collection, id);
            }
            if (mutation) {
                return transaction_document(*collection,
                                            id,
                                            mutation->content<nlohmann::json>(),
                                            0,
                                            transaction_document_status::OWN_WRITE,
                                            transaction_links(mutation->doc().links()));
            }
            mutation = staged_mutations_.find_remove(collection, id);
            if (mutation) {
                throw std::runtime_error(std::string("not found"));
            }

            boost::optional<transaction_document> out;
            LOG(txctx_, trace) << "getting doc " << id;
            const result& res = collection->lookup_in(id,
                                                      { lookup_in_spec::get(ATR_ID).xattr(),
                                                        lookup_in_spec::get(STAGED_VERSION).xattr(),
                                                        lookup_in_spec::get(STAGED_DATA).xattr(),
                                                        lookup_in_spec::get(ATR_BUCKET_NAME).xattr(),
                                                        lookup_in_spec::get(ATR_SCOPE_NAME).xattr(),
                                                        lookup_in_spec::get(ATR_COLL_NAME).xattr(),
                                                        lookup_in_spec::fulldoc_get() });
            if (res.rc == LCB_SUCCESS) {
                std::string atr_id = res.values[0]->get<std::string>();
                std::string staged_version = res.values[1]->get<std::string>();
                nlohmann::json staged_data = *res.values[2];
                std::string atr_bucket_name = res.values[3]->get<std::string>();
                std::string atr_scope_name = res.values[4]->get<std::string>();
                std::string atr_coll_name = res.values[5]->get<std::string>();
                nlohmann::json content = *res.values[6];

                transaction_document doc(*collection,
                                         id,
                                         content,
                                         res.cas,
                                         transaction_document_status::NORMAL,
                                         transaction_links(atr_id, atr_bucket_name, atr_scope_name, atr_coll_name, content, id));

                if (doc.links().is_document_in_transaction()) {
                    const result& atr_res =
                      collection->lookup_in(doc.links().atr_id(), { lookup_in_spec::get(ATR_FIELD_ATTEMPTS).xattr() });
                    if (atr_res.rc != LCB_ERR_DOCUMENT_NOT_FOUND && atr_res.values[0]) {
                        std::string err;
                        const nlohmann::json& atr = *atr_res.values[0];
                        const nlohmann::json& entry = atr[id_];
                        if (entry == nullptr) {
                            // Don't know if txn was committed or rolled back.  Should not happen as ATR record should stick around long
                            // enough.
                            doc.status(transaction_document_status::AMBIGUOUS);
                            if (doc.content<nlohmann::json>().is_null()) {
                                throw std::runtime_error(std::string("not found"));
                            }
                        } else {
                            if (doc.links().staged_version().empty()) {
                                if (entry["status"] == "COMMITTED") {
                                    if (doc.links().is_document_being_removed()) {
                                        throw std::runtime_error(std::string("not found"));
                                    } else {
                                        doc.content(doc.links().staged_content<nlohmann::json>());
                                        doc.status(transaction_document_status::IN_TXN_COMMITTED);
                                    }
                                } else {
                                    doc.status(transaction_document_status::IN_TXN_OTHER);
                                    if (doc.content<nlohmann::json>().is_null()) {
                                        throw std::runtime_error(std::string("not found"));
                                    }
                                }
                            } else {
                                doc.content(doc.links().staged_content<nlohmann::json>());
                                doc.status(transaction_document_status::OWN_WRITE);
                            }
                        }
                    }
                }
                LOG(txctx_, trace) << "completed get of " << doc;
                out.emplace(doc);
                return out;
            }
            LOG(txctx_, warning) << "got error while getting doc " << id << ": " << lcb_strerror_short(res.rc);
            return out;
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
        transaction_document replace(collection* collection, const transaction_document& document, const Content& content)
        {
            init_atr_if_needed(collection, document.id());

            if (staged_mutations_.empty()) {
                std::string prefix(ATR_FIELD_ATTEMPTS + "." + id_ + ".");
                const result& res = collection->mutate_in(
                  atr_id_.value(),
                  {
                    mutate_in_spec::insert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::PENDING)).xattr().create_path(),
                    mutate_in_spec::insert(prefix + ATR_FIELD_START_TIMESTAMP, "${Mutation.CAS}").xattr().expand_macro(),
                    mutate_in_spec::insert(prefix + ATR_FIELD_EXPIRES_AFTER_MSECS, 15).xattr(),
                    mutate_in_spec::fulldoc_upsert(nlohmann::json::object()),
                  },
                  durability(config_));
                if (res.rc != LCB_SUCCESS) {
                    throw std::runtime_error(std::string("failed to set ATR to pending state: ") + lcb_strerror_short(res.rc));
                }
            }

            LOG(txctx_, trace) << "replacing doc " << document.id();
            const result& res = collection->mutate_in(document.id(),
                                                      {
                                                        mutate_in_spec::upsert(STAGED_VERSION, id_).xattr().create_path(),
                                                        mutate_in_spec::upsert(ATR_ID, atr_id_.value()).xattr(),
                                                        mutate_in_spec::upsert(STAGED_DATA, content).xattr(),
                                                        mutate_in_spec::upsert(ATR_BUCKET_NAME, collection->bucket_name()).xattr(),
                                                        mutate_in_spec::upsert(ATR_SCOPE_NAME, collection->scope()).xattr(),
                                                        mutate_in_spec::upsert(ATR_COLL_NAME, collection->name()).xattr(),
                                                      },
                                                      durability(config_));

            if (res.rc == LCB_SUCCESS) {
                transaction_document out(
                  *collection,
                  document.id(),
                  document.content<nlohmann::json>(),
                  res.cas,
                  transaction_document_status::NORMAL,
                  transaction_links(atr_id_.value(), collection->bucket_name(), collection->scope(), collection->name(), content, id_));
                staged_mutations_.add(staged_mutation(out, content, staged_mutation_type::REPLACE));
                return out;
            }
            throw std::runtime_error(std::string("failed to replace the document: ") + lcb_strerror_short(res.rc));
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
        transaction_document insert(collection* collection, const std::string& id, const Content& content)
        {
            init_atr_if_needed(collection, id);

            if (staged_mutations_.empty()) {
                std::string prefix(ATR_FIELD_ATTEMPTS + "." + id_ + ".");
                collection->mutate_in(
                  id,
                  {
                    mutate_in_spec::insert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::PENDING)).xattr().create_path(),
                    mutate_in_spec::insert(prefix + ATR_FIELD_START_TIMESTAMP, "${Mutation.CAS}").xattr().expand_macro(),
                    mutate_in_spec::insert(prefix + ATR_FIELD_EXPIRES_AFTER_MSECS, 15).xattr(),
                    mutate_in_spec::fulldoc_upsert(nlohmann::json::object()),
                  },
                  durability(config_));
            }

            LOG(txctx_, trace) << "inserting doc " << id;
            const result& res = collection->mutate_in(id,
                                                      {
                                                        mutate_in_spec::upsert(STAGED_VERSION, id_).xattr().create_path(),
                                                        mutate_in_spec::insert(ATR_ID, atr_id_.value()).xattr(),
                                                        mutate_in_spec::insert(STAGED_DATA, content).xattr(),
                                                        mutate_in_spec::insert(ATR_BUCKET_NAME, collection->bucket_name()).xattr(),
                                                        mutate_in_spec::insert(ATR_SCOPE_NAME, collection->scope()).xattr(),
                                                        mutate_in_spec::insert(ATR_COLL_NAME, collection->name()).xattr(),
                                                        mutate_in_spec::fulldoc_insert(nlohmann::json::object()),
                                                      },
                                                      durability(config_));
            if (res.rc == LCB_SUCCESS) {
                transaction_document out(
                  *collection,
                  id,
                  content,
                  res.cas,
                  transaction_document_status::NORMAL,
                  transaction_links(atr_id_.value(), collection->bucket_name(), collection->scope(), collection->name(), content, id_));
                staged_mutations_.add(staged_mutation(out, content, staged_mutation_type::INSERT));
                return out;
            }
            throw std::runtime_error(std::string("failed to insert the document: ") + lcb_strerror_short(res.rc));
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
        void remove(collection* collection, transaction_document& document)
        {
            init_atr_if_needed(collection, document.id());

            if (staged_mutations_.empty()) {
                std::string prefix(ATR_FIELD_ATTEMPTS + "." + id_ + ".");
                collection->mutate_in(
                  document.id(),
                  {
                    mutate_in_spec::insert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::PENDING)).xattr().create_path(),
                    mutate_in_spec::insert(prefix + ATR_FIELD_START_TIMESTAMP, "${Mutation.CAS}").xattr().expand_macro(),
                    mutate_in_spec::insert(prefix + ATR_FIELD_EXPIRES_AFTER_MSECS, 15).xattr(),
                    mutate_in_spec::fulldoc_upsert(nlohmann::json::object()),
                  },
                  durability(config_));
            }

            LOG(txctx_, trace) << "removing doc " << document.id();
            const result& res = collection->mutate_in(document.id(),
                                                      {
                                                        mutate_in_spec::upsert(STAGED_VERSION, id_).xattr().create_path(),
                                                        mutate_in_spec::upsert(ATR_ID, atr_id_.value()).xattr(),
                                                        mutate_in_spec::upsert(STAGED_DATA, STAGED_DATA_REMOVED_VALUE).xattr(),
                                                        mutate_in_spec::upsert(ATR_BUCKET_NAME, collection->bucket_name()).xattr(),
                                                        mutate_in_spec::upsert(ATR_SCOPE_NAME, collection->scope()).xattr(),
                                                        mutate_in_spec::upsert(ATR_COLL_NAME, collection->name()).xattr(),
                                                      },
                                                      durability(config_));
            if (res.rc == LCB_SUCCESS) {
                document.cas(res.cas);
                staged_mutations_.add(staged_mutation(document, "", staged_mutation_type::REMOVE));
                return;
            }
            throw std::runtime_error(std::string("failed to remove the document: ") + lcb_strerror_short(res.rc));
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
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + id_ + ".");
            std::vector<mutate_in_spec> specs({
              mutate_in_spec::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::COMMITTED)).xattr(),
              mutate_in_spec::upsert(prefix + ATR_FIELD_START_COMMIT, "${Mutation.CAS}").xattr().expand_macro(),
            });
            staged_mutations_.extract_to(prefix, specs);
            const result& res = atr_collection_->mutate_in(atr_id_.value(), specs);
            if (res.rc == LCB_SUCCESS) {
                std::vector<transaction_document> docs;
                staged_mutations_.commit();
                is_done_ = true;
                state_ = attempt_state::COMMITTED;
            } else {
                throw std::runtime_error(std::string("failed to commit transaction: ") + id_ + ": " + lcb_strerror_short(res.rc));
            }
        }

        [[nodiscard]] bool is_done()
        {
            return is_done_;
        }

        [[nodiscard]] const std::string& id()
        {
            return id_;
        }

      private:
        static lcb_DURABILITY_LEVEL durability(const configuration& config)
        {
            switch (config.durability_level()) {
                case durability_level::NONE:
                    return LCB_DURABILITYLEVEL_NONE;
                case durability_level::MAJORITY:
                    return LCB_DURABILITYLEVEL_MAJORITY;
                case durability_level::MAJORITY_AND_PERSIST_TO_ACTIVE:
                    return LCB_DURABILITYLEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE;
                case durability_level::PERSIST_TO_MAJORITY:
                    return LCB_DURABILITYLEVEL_PERSIST_TO_MAJORITY;
            }
            throw std::runtime_error("unknown durability");
        }
    };
} // namespace transactions
} // namespace couchbase
