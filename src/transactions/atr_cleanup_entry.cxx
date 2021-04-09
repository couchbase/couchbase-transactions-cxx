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

#include "atr_cleanup_entry.hxx"
#include "active_transaction_record.hxx"
#include "attempt_context_impl.hxx"
#include "attempt_context_testing_hooks.hxx"
#include "cleanup_testing_hooks.hxx"
#include "forward_compat.hxx"
#include "logging.hxx"
#include "transactions_cleanup.hxx"
#include "utils.hxx"

#include <boost/optional/optional_io.hpp>

#include <couchbase/transactions.hxx>
#include <couchbase/transactions/exceptions.hxx>

namespace tx = couchbase::transactions;

// NOTE: priority queue outputs largest to smallest - since we want the least
// recent statr time first, this returns true if lhs > rhs
bool
tx::compare_atr_entries::operator()(atr_cleanup_entry& lhs, atr_cleanup_entry& rhs)
{
    return lhs.min_start_time_ > rhs.min_start_time_;
}
// wait a bit after an attempt is expired before cleaning it.
const uint32_t tx::atr_cleanup_entry::safety_margin_ms_ = 1500;

tx::atr_cleanup_entry::atr_cleanup_entry(const std::string& atr_id,
                                         const std::string& attempt_id,
                                         std::shared_ptr<couchbase::collection> atr_coll,
                                         const transactions_cleanup& cleanup)
  : atr_id_(atr_id)
  , attempt_id_(attempt_id)
  , check_if_expired_(false)
  , atr_collection_(atr_coll)
  , atr_entry_(nullptr)
  , cleanup_(&cleanup)
{
}

tx::atr_cleanup_entry::atr_cleanup_entry(const atr_entry& entry,
                                         std::shared_ptr<couchbase::collection> atr_coll,
                                         const transactions_cleanup& cleanup,
                                         bool check_if_expired)
  : atr_id_(entry.atr_id())
  , attempt_id_(entry.attempt_id())
  , check_if_expired_(check_if_expired)
  , atr_collection_(atr_coll)
  , atr_entry_(&entry)
  , cleanup_(&cleanup)
{
}

tx::atr_cleanup_entry::atr_cleanup_entry(attempt_context& ctx)
  : min_start_time_(std::chrono::steady_clock::now())
  , check_if_expired_(false)
  , atr_entry_(nullptr)
{
    // NOTE: we create these entries externally, in fit_performer tests, hence the
    // use of attempt_context rather than attempt_context_impl
    auto& ctx_impl = static_cast<attempt_context_impl&>(ctx);
    atr_id_ = ctx_impl.atr_id();
    attempt_id_ = ctx_impl.id();
    cleanup_ = &ctx_impl.parent_->cleanup();

    // need the collection to be safe to use in cleanup thread, so get it from
    // the cleanup's cluster.
    atr_collection_ = cleanup_->cluster().bucket(ctx_impl.atr_collection_->bucket_name())->collection(ctx_impl.atr_collection_->name());
}

void
tx::atr_cleanup_entry::clean(std::shared_ptr<spdlog::logger> logger, transactions_cleanup_attempt* result)
{
    logger->trace("cleaning {}", *this);
    // get atr entry if needed
    atr_entry entry;
    if (nullptr == atr_entry_) {
        auto atr = tx::active_transaction_record::get_atr(atr_collection_, atr_id_);
        if (atr) {
            // now get the specific attempt
            auto it =
              std::find_if(atr->entries().begin(), atr->entries().end(), [&](const atr_entry& e) { return e.attempt_id() == attempt_id_; });
            if (it != atr->entries().end()) {
                atr_entry_ = &(*it);
                return check_atr_and_cleanup(logger, result);
            } else {
                logger->trace("could not find attempt {}, nothing to clean", attempt_id_);
                return;
            }
        } else {
            logger->trace("could not find atr {} in collection {}, nothing to clean", atr_id_, atr_collection_->name());
            return;
        }
    }
    check_atr_and_cleanup(logger, result);
}

void
tx::atr_cleanup_entry::check_atr_and_cleanup(std::shared_ptr<spdlog::logger> logger, transactions_cleanup_attempt* result)
{
    if (check_if_expired_ && !atr_entry_->has_expired(safety_margin_ms_)) {
        logger->trace("{} not expired, nothing to clean", *this);
        return;
    }
    if (result) {
        result->state(atr_entry_->state());
    }
    forward_compat::check(forward_compat_stage::CLEANUP_ENTRY, atr_entry_->forward_compat());
    cleanup_docs(logger);
    cleanup_->config().cleanup_hooks().on_cleanup_docs_completed();
    cleanup_entry(logger);
    cleanup_->config().cleanup_hooks().on_cleanup_completed();
    return;
}

void
tx::atr_cleanup_entry::cleanup_docs(std::shared_ptr<spdlog::logger> logger)
{
    switch (atr_entry_->state()) {
        case tx::attempt_state::COMMITTED:
            commit_docs(logger, atr_entry_->inserted_ids());
            commit_docs(logger, atr_entry_->replaced_ids());
            remove_docs_staged_for_removal(logger, atr_entry_->removed_ids());
            break;
        // half-finished commit
        case tx::attempt_state::ABORTED:
            // half finished rollback
            remove_docs(logger, atr_entry_->inserted_ids());
            remove_txn_links(logger, atr_entry_->replaced_ids());
            remove_txn_links(logger, atr_entry_->removed_ids());
            break;
        default:
            logger->trace("attempt in {}, nothing to do in cleanup_docs", attempt_state_name(atr_entry_->state()));
    }
}

void
tx::atr_cleanup_entry::do_per_doc(std::shared_ptr<spdlog::logger> logger,
                                  std::vector<tx::doc_record> docs,
                                  bool require_crc_to_match,
                                  const std::function<void(std::shared_ptr<spdlog::logger>, transaction_document&, bool)>& call)
{
    for (auto& dr : docs) {
        auto collection = cleanup_->cluster().bucket(dr.bucket_name())->collection(dr.collection_name());
        try {
            couchbase::result res;
            cleanup_->config().cleanup_hooks().before_doc_get(dr.id());
            tx::wrap_collection_call(res, [&](result& r) {
                r = collection->lookup_in(dr.id(),
                                          { lookup_in_spec::get(ATR_ID).xattr(),
                                            lookup_in_spec::get(TRANSACTION_ID).xattr(),
                                            lookup_in_spec::get(ATTEMPT_ID).xattr(),
                                            lookup_in_spec::get(STAGED_DATA).xattr(),
                                            lookup_in_spec::get(ATR_BUCKET_NAME).xattr(),
                                            lookup_in_spec::get(ATR_COLL_NAME).xattr(),
                                            lookup_in_spec::get(TRANSACTION_RESTORE_PREFIX_ONLY).xattr(),
                                            lookup_in_spec::get(TYPE).xattr(),
                                            lookup_in_spec::get("$document").xattr(),
                                            lookup_in_spec::get(CRC32_OF_STAGING).xattr(),
                                            lookup_in_spec::get(FORWARD_COMPAT).xattr(),
                                            lookup_in_spec::fulldoc_get() },
                                          lookup_in_options().access_deleted(true));
            });
            if (res.values.empty()) {
                logger->trace("cannot create a transaction document from {}, ignoring", res);
                continue;
            }
            transaction_document doc = transaction_document::create_from(*collection, dr.id(), res);
            // now lets decide if we call the function or not
            if (!(doc.links().has_staged_content() || doc.links().is_document_being_removed()) || !doc.links().has_staged_write()) {
                logger->trace("document {} has no staged content - assuming it was "
                              "committed and skipping",
                              dr.id());
                continue;
            } else if (doc.links().staged_attempt_id() != attempt_id_) {
                logger->trace("document {} staged for different attempt {}, skipping", dr.id(), doc.links().staged_attempt_id());
                continue;
            }
            if (require_crc_to_match) {
                if (!doc.metadata()->crc32() || !doc.links().crc32_of_staging() ||
                    doc.links().crc32_of_staging() != doc.metadata()->crc32()) {
                    logger->trace("document {} crc32 {} doesn't match staged value {}, skipping",
                                  dr.id(),
                                  doc.metadata()->crc32(),
                                  doc.links().crc32_of_staging());
                    continue;
                }
            }
            call(logger, doc, res.is_deleted);
        } catch (const client_error& e) {
            error_class ec = e.ec();
            switch (ec) {
                case FAIL_DOC_NOT_FOUND:
                    logger->error("document {} not found - ignoring ", dr);
                    break;
                default:
                    logger->error("got error {}, not ignoring this", e.what());
                    throw;
            }
        }
    }
}

void
tx::atr_cleanup_entry::commit_docs(std::shared_ptr<spdlog::logger> logger, boost::optional<std::vector<tx::doc_record>> docs)
{
    if (docs) {
        do_per_doc(logger, *docs, true, [&](std::shared_ptr<spdlog::logger> logger, tx::transaction_document& doc, bool is_deleted) {
            if (doc.links().has_staged_content()) {
                nlohmann::json content = doc.links().staged_content<nlohmann::json>();
                cleanup_->config().cleanup_hooks().before_commit_doc(doc.id());
                couchbase::result res;
                tx::wrap_collection_call(res, [&](result& r) {
                    if (doc.links().is_deleted()) {
                        r = doc.collection_ref().insert(doc.id(), content);
                    } else {
                        r = doc.collection_ref().mutate_in(
                          doc.id(),
                          { mutate_in_spec::remove(TRANSACTION_INTERFACE_PREFIX_ONLY).xattr(), mutate_in_spec::fulldoc_upsert(content) },
                          wrap_option(mutate_in_options(), cleanup_->config())
                            .cas(doc.cas())
                            .store_semantics(subdoc_store_semantics::replace));
                    }
                });
                logger->trace("commit_docs replaced content of doc {} with {}", doc.id(), content.dump());
            } else {
                logger->trace("commit_docs skipping document {}, no staged content", doc.id());
            }
        });
    }
}
void
tx::atr_cleanup_entry::remove_docs(std::shared_ptr<spdlog::logger> logger, boost::optional<std::vector<tx::doc_record>> docs)
{
    if (docs) {
        do_per_doc(logger, *docs, true, [&](std::shared_ptr<spdlog::logger> logger, transaction_document& doc, bool is_deleted) {
            cleanup_->config().cleanup_hooks().before_remove_doc(doc.id());
            couchbase::result res;
            tx::wrap_collection_call(res, [&](result& r) {
                if (is_deleted) {
                    r = doc.collection_ref().mutate_in(
                      doc.id(),
                      { mutate_in_spec::remove(TRANSACTION_INTERFACE_PREFIX_ONLY).xattr() },
                      wrap_option(mutate_in_options(), cleanup_->config()).access_deleted(true).cas(doc.cas()));
                } else {
                    r = doc.collection_ref().remove(doc.id(), remove_options().cas(doc.cas()));
                }
            });
            logger->trace("remove_docs removed doc {}", doc.id());
        });
    }
}

void
tx::atr_cleanup_entry::remove_docs_staged_for_removal(std::shared_ptr<spdlog::logger> logger,
                                                      boost::optional<std::vector<tx::doc_record>> docs)
{
    if (docs) {
        do_per_doc(logger, *docs, true, [&](std::shared_ptr<spdlog::logger> logger, transaction_document& doc, bool) {
            couchbase::result res;
            tx::wrap_collection_call(res, [&](result& r) {
                if (doc.links().is_document_being_removed()) {
                    cleanup_->config().cleanup_hooks().before_remove_doc_staged_for_removal(doc.id());
                    r = doc.collection_ref().remove(doc.id(), remove_options().cas(doc.cas()));
                    logger->trace("remove_docs_staged_for_removal removed doc {}", doc.id());
                } else {
                    logger->trace("remove_docs_staged_for_removal found document {} not "
                                  "marked for removal, skipping",
                                  doc.id());
                }
            });
        });
    }
}

void
tx::atr_cleanup_entry::remove_txn_links(std::shared_ptr<spdlog::logger> logger, boost::optional<std::vector<tx::doc_record>> docs)
{
    if (docs) {
        do_per_doc(logger, *docs, false, [&](std::shared_ptr<spdlog::logger> logger, transaction_document& doc, bool) {
            couchbase::result res;
            tx::wrap_collection_call(res, [&](result& r) {
                cleanup_->config().cleanup_hooks().before_remove_links(doc.id());
                r =
                  doc.collection_ref().mutate_in(doc.id(),
                                                 {
                                                   mutate_in_spec::upsert(TRANSACTION_INTERFACE_PREFIX_ONLY, nullptr).xattr(),
                                                   mutate_in_spec::remove(TRANSACTION_INTERFACE_PREFIX_ONLY).xattr(),
                                                 },
                                                 wrap_option(mutate_in_options(), cleanup_->config()).access_deleted(true).cas(doc.cas()));
                logger->trace("remove_txn_links removed links for doc {}", doc.id());
            });
        });
    }
}

void
tx::atr_cleanup_entry::cleanup_entry(std::shared_ptr<spdlog::logger> logger)
{
    try {
        cleanup_->config().cleanup_hooks().before_atr_remove();
        couchbase::result res;
        auto coll = atr_collection_;
        tx::wrap_collection_call(res, [&](result& r) {
            std::string path("attempts.");
            path += attempt_id_;
            r = coll->mutate_in(atr_id_,
                                { mutate_in_spec::upsert(path, nullptr).xattr(), mutate_in_spec::remove(path).xattr() },
                                wrap_option(mutate_in_options(), cleanup_->config()));
        });
        logger->trace("successfully removed attempt {}", attempt_id_);
    } catch (const client_error& e) {
        logger->error("cleanup couldn't remove attempt {} due to {}", attempt_id_, e.what());
        throw;
    }
}

bool
tx::atr_cleanup_entry::ready() const
{
    return std::chrono::steady_clock::now() > min_start_time_;
}

boost::optional<tx::atr_cleanup_entry>
tx::atr_cleanup_queue::pop(bool check_time)
{
    std::unique_lock<std::mutex> lock(mutex_);
    if (!queue_.empty()) {
        if (!check_time || (check_time && queue_.top().ready())) {
            // copy it
            tx::atr_cleanup_entry top = queue_.top();
            // pop it
            queue_.pop();
            return { top };
        }
    }
    return {};
}

int
tx::atr_cleanup_queue::size() const
{
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.size();
}

void
tx::atr_cleanup_queue::push(attempt_context& ctx)
{
    std::unique_lock<std::mutex> lock(mutex_);
    queue_.emplace(ctx);
}

void
tx::atr_cleanup_queue::push(const atr_cleanup_entry& e)
{
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.push(e);
}
