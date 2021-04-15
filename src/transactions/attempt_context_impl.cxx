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

#include "attempt_context_impl.hxx"
#include "active_transaction_record.hxx"
#include "atr_ids.hxx"
#include "attempt_context_testing_hooks.hxx"
#include "exceptions_internal.hxx"
#include "forward_compat.hxx"
#include "logging.hxx"
#include "staged_mutation.hxx"
#include "utils.hxx"

#include <couchbase/transactions.hxx>
#include <couchbase/transactions/attempt_state.hxx>

namespace couchbase
{
namespace transactions
{

    void attempt_context_impl::existing_error()
    {
        if (!errors_.empty()) {
            throw transaction_operation_failed(FAIL_OTHER, "Previous operation failed").cause(PREVIOUS_OPERATION_FAILED);
        }
    }

    attempt_context_impl::attempt_context_impl(transactions* parent, transaction_context& transaction_ctx, const transaction_config& config)
      : overall_(transaction_ctx)
      , config_(config)
      , parent_(parent)
      , atr_collection_(nullptr)
      , is_done_(false)
      , hooks_(config.attempt_context_hooks())
      , staged_mutations_(new staged_mutation_queue())
    {
        // put a new transaction_attempt in the context...
        overall_.add_attempt();
        trace("added new attempt, state {}", state());
    }

    attempt_context_impl::~attempt_context_impl() = default;

    // not a member of attempt_context_impl, as forward_compat is internal.
    void attempt_context_impl::check_and_handle_blocking_transactions(const transaction_get_result& doc, forward_compat_stage stage)
    {
        // The main reason to require doc to be fetched inside the transaction is so we can detect this on the client side
        if (doc.links().has_staged_write()) {
            // Check not just writing the same doc twice in the same transaction
            // NOTE: we check the transaction rather than attempt id. This is to handle [RETRY-ERR-AMBIG-REPLACE].
            if (doc.links().staged_transaction_id().value() == transaction_id()) {
                debug("doc {} has been written by this transaction, ok to continue", doc.id());
            } else {
                if (doc.links().atr_id() && doc.links().atr_bucket_name() && doc.links().staged_attempt_id()) {
                    debug("doc {} in another txn, checking atr...", doc.id());
                    forward_compat::check(stage, doc.links().forward_compat());
                    check_atr_entry_for_blocking_document(doc);
                } else {
                    debug("doc {} is in another transaction {}, but doesn't have enough info to check the atr. "
                          "probably a bug, proceeding to overwrite",
                          doc.id(),
                          doc.links().staged_attempt_id().get());
                }
            }
        }
    }

    transaction_get_result attempt_context_impl::get(std::shared_ptr<couchbase::collection> collection, const std::string& id)
    {
        return cache_error<transaction_get_result>([&]() {
            auto result = get_optional(collection, id);
            if (result) {
                trace("get returning {}", *result);
                return result.get();
            }
            error("Document with id {} not found", id);
            throw transaction_operation_failed(FAIL_DOC_NOT_FOUND, "Document not found");
        });
    }

    boost::optional<transaction_get_result> attempt_context_impl::get_optional(std::shared_ptr<couchbase::collection> collection,
                                                                               const std::string& id)
    {
        return cache_error<boost::optional<transaction_get_result>>([&]() {
            auto retval = do_get(collection, id);
            hooks_.after_get_complete(this, id);
            if (retval) {
                forward_compat::check(forward_compat_stage::GETS, retval->links().forward_compat());
            }
            return retval;
        });
    }

    transaction_get_result attempt_context_impl::replace_raw(std::shared_ptr<couchbase::collection> collection,
                                                             const transaction_get_result& document,
                                                             const nlohmann::json& content)
    {
        return retry_op_exp<transaction_get_result>([&]() -> transaction_get_result {
            return cache_error<transaction_get_result>([&]() {
                try {
                    trace("replacing {} with {}", document, content.dump());
                    check_if_done();
                    check_expiry_pre_commit(STAGE_REPLACE, document.id());
                    select_atr_if_needed(collection, document.id());
                    check_and_handle_blocking_transactions(document, forward_compat_stage::WWC_REPLACING);
                    set_atr_pending_if_first_mutation(collection);

                    std::vector<mutate_in_spec> specs = {
                        mutate_in_spec::upsert(TRANSACTION_ID, overall_.transaction_id()).xattr().create_path(),
                        mutate_in_spec::upsert(ATTEMPT_ID, id()).xattr().create_path(),
                        mutate_in_spec::upsert(STAGED_DATA, content).xattr().create_path(),
                        mutate_in_spec::upsert(ATR_ID, atr_id_.value()).xattr().create_path(),
                        mutate_in_spec::upsert(ATR_BUCKET_NAME, collection->bucket_name()).create_path().xattr(),
                        mutate_in_spec::upsert(ATR_COLL_NAME, collection->scope() + "." + collection->name()).create_path().xattr(),
                        mutate_in_spec::upsert(CRC32_OF_STAGING, mutate_in_macro::VALUE_CRC_32C)
                          .xattr()
                          .create_path()
                          .create_path()
                          .expand_macro(),
                        mutate_in_spec::upsert(TYPE, "replace").xattr(),
                    };
                    if (document.metadata()) {
                        if (document.metadata()->cas()) {
                            specs.emplace_back(
                              mutate_in_spec::upsert(PRE_TXN_CAS, document.metadata()->cas().value()).create_path().xattr());
                        }
                        if (document.metadata()->revid()) {
                            specs.emplace_back(
                              mutate_in_spec::upsert(PRE_TXN_REVID, document.metadata()->revid().value()).create_path().xattr());
                        }
                        if (document.metadata()->exptime()) {
                            specs.emplace_back(
                              mutate_in_spec::upsert(PRE_TXN_EXPTIME, document.metadata()->exptime().value()).create_path().xattr());
                        }
                    }

                    couchbase::result res;
                    hooks_.before_staged_replace(this, document.id());
                    trace("about to replace doc {} with cas {} in txn {}", document.id(), document.cas(), overall_.transaction_id());
                    wrap_collection_call(res, [&](couchbase::result& r) {
                        r = collection->mutate_in(
                          document.id(),
                          specs,
                          wrap_option(mutate_in_options(), config_).cas(document.cas()).access_deleted(document.links().is_deleted()));
                    });
                    hooks_.after_staged_replace_complete(this, document.id());
                    transaction_get_result out = document;
                    out.cas(res.cas);
                    trace("replace staged content, result {}", res);
                    staged_mutation* existing_replace = staged_mutations_->find_replace(collection, document.id());
                    staged_mutation* existing_insert = staged_mutations_->find_insert(collection, document.id());
                    if (existing_replace != nullptr) {
                        trace("document {} was replaced already in txn, replacing again", document.id());
                        // only thing that we need to change are the content, cas
                        existing_replace->content(content);
                        existing_replace->doc().cas(out.cas());
                    } else if (existing_insert != nullptr) {
                        trace("document {} replaced after insert in this txn", document.id());
                        // only thing that we need to change are the content, cas
                        existing_insert->doc().content(content);
                        existing_insert->doc().cas(out.cas());
                    } else {
                        staged_mutations_->add(staged_mutation(out, content, staged_mutation_type::REPLACE));
                    }
                    return out;
                } catch (const client_error& e) {
                    error_class ec = e.ec();
                    switch (ec) {
                        case FAIL_EXPIRY:
                            expiry_overtime_mode_ = true;
                            throw transaction_operation_failed(ec, e.what()).expired();
                        case FAIL_DOC_NOT_FOUND:
                        case FAIL_DOC_ALREADY_EXISTS:
                        case FAIL_CAS_MISMATCH:
                            throw transaction_operation_failed(ec, e.what()).retry();
                        case FAIL_TRANSIENT:
                        case FAIL_AMBIGUOUS:
                            throw transaction_operation_failed(ec, e.what()).retry();
                        case FAIL_HARD:
                            throw transaction_operation_failed(ec, e.what()).no_rollback();
                        default:
                            throw transaction_operation_failed(ec, e.what());
                    }
                }
            });
        });
    }

    transaction_get_result attempt_context_impl::insert_raw(std::shared_ptr<couchbase::collection> collection,
                                                            const std::string& id,
                                                            const nlohmann::json& content)
    {
        return cache_error<transaction_get_result>([&]() {
            try {
                check_if_done();
                if (check_for_own_write(collection, id)) {
                    throw transaction_operation_failed(FAIL_OTHER,
                                                       "cannot insert a document that has already been mutated in this transaction");
                }
                check_expiry_pre_commit(STAGE_INSERT, id);
                select_atr_if_needed(collection, id);
                set_atr_pending_if_first_mutation(collection);
                uint64_t cas = 0;
                return retry_op<transaction_get_result>(
                  [&]() -> transaction_get_result { return create_staged_insert(collection, id, content, cas); });
            } catch (const client_error& e) {
                error_class ec = e.ec();
                if (expiry_overtime_mode_) {
                    throw transaction_operation_failed(FAIL_EXPIRY, "attempt timed out").expired();
                }
                switch (ec) {
                    case FAIL_EXPIRY:
                        expiry_overtime_mode_ = true;
                        throw transaction_operation_failed(ec, "attempt timed-out").expired();
                    case FAIL_TRANSIENT:
                        throw transaction_operation_failed(ec, "transient error in insert").retry();
                    case FAIL_OTHER:
                        throw transaction_operation_failed(ec, e.what());
                    case FAIL_HARD:
                        throw transaction_operation_failed(ec, e.what()).no_rollback();
                    default:
                        throw transaction_operation_failed(FAIL_OTHER, e.what()).retry();
                }
            }
        });
    }

    void attempt_context_impl::select_atr_if_needed(std::shared_ptr<couchbase::collection> collection, const std::string& id)
    {
        if (!atr_id_) {
            int vbucket_id = -1;
            boost::optional<const std::string> hook_atr = hooks_.random_atr_id_for_vbucket(this);
            if (hook_atr) {
                atr_id_.emplace(*hook_atr);
            } else {
                vbucket_id = atr_ids::vbucket_for_key(id);
                atr_id_.emplace(atr_ids::atr_id_for_vbucket(vbucket_id));
            }
            atr_collection_ = collection->get_bucket()->default_collection();
            overall_.atr_collection(collection->name());
            overall_.atr_id(*atr_id_);
            state(attempt_state::NOT_STARTED);
            trace("first mutated doc in transaction is \"{}\" on vbucket {}, so using atr \"{}\"", id, vbucket_id, atr_id_.value());
        }
    }

    void attempt_context_impl::check_atr_entry_for_blocking_document(const transaction_get_result& doc)
    {
        auto collection = parent_->cluster_ref().bucket(doc.links().atr_bucket_name().value())->default_collection();
        try {
            retry_op_exponential_backoff_timeout<void>(
              std::chrono::milliseconds(50), std::chrono::milliseconds(500), std::chrono::seconds(1), [&] {
                  try {
                      hooks_.before_check_atr_entry_for_blocking_doc(this, doc.id());
                      auto atr = active_transaction_record::get_atr(collection, doc.links().atr_id().value());
                      if (atr) {
                          auto entries = atr->entries();
                          auto it = std::find_if(entries.begin(), entries.end(), [&doc](const atr_entry& e) {
                              return e.attempt_id() == doc.links().staged_attempt_id();
                          });
                          if (it != entries.end()) {
                              forward_compat::check(forward_compat_stage::WWC_READING_ATR, it->forward_compat());
                              if (it->has_expired()) {
                                  debug("existing atr entry has expired (age is {}ms), ignoring", it->age_ms());
                                  return;
                              }
                              switch (it->state()) {
                                  case attempt_state::COMPLETED:
                                  case attempt_state::ROLLED_BACK:
                                      debug("existing atr entry can be ignored due to state {}", it->state());
                                      return;
                                  default:
                                      debug("existing atr entry found in state {}, retrying", it->state());
                              }
                              throw retry_operation("retry check for blocking doc");
                          } else {
                              debug("no blocking atr entry");
                              return;
                          }
                      } else {
                          debug("atr entry not found, assuming we can proceed");
                          return;
                      }
                      // if we are here, there is still a write-write conflict
                      throw transaction_operation_failed(FAIL_WRITE_WRITE_CONFLICT, "document is in another transaction").retry();
                  } catch (const client_error& e) {
                      throw transaction_operation_failed(FAIL_WRITE_WRITE_CONFLICT, e.what()).retry();
                  }
              });
        } catch (const retry_operation_timeout& t) {
            throw transaction_operation_failed(FAIL_WRITE_WRITE_CONFLICT, t.what()).retry();
        }
    }

    void attempt_context_impl::remove(std::shared_ptr<collection> collection, transaction_get_result& document)
    {
        return cache_error<void>([&]() {
            try {
                check_if_done();
                check_expiry_pre_commit(STAGE_REMOVE, document.id());
                if (staged_mutations_->find_insert(collection, document.id())) {
                    error("cannot remove document {}, as it was inserted in this transaction", document.id());
                    throw transaction_operation_failed(FAIL_OTHER, "Cannot remove a document inserted in the same transaction");
                }
                trace("removing {}", document);
                check_and_handle_blocking_transactions(document, forward_compat_stage::WWC_REMOVING);
                select_atr_if_needed(collection, document.id());

                set_atr_pending_if_first_mutation(collection);

                hooks_.before_staged_remove(this, document.id());
                trace("about to remove remove doc {} with cas {}", document.id(), document.cas());
                std::vector<mutate_in_spec> specs = {
                    mutate_in_spec::upsert(TRANSACTION_ID, overall_.transaction_id()).xattr().create_path(),
                    mutate_in_spec::upsert(ATTEMPT_ID, id()).create_path().xattr(),
                    mutate_in_spec::upsert(ATR_ID, atr_id_.value()).create_path().xattr(),
                    mutate_in_spec::upsert(ATR_BUCKET_NAME, collection->bucket_name()).create_path().xattr(),
                    mutate_in_spec::upsert(ATR_COLL_NAME, collection->scope() + "." + collection->name()).create_path().xattr(),
                    mutate_in_spec::upsert(CRC32_OF_STAGING, mutate_in_macro::VALUE_CRC_32C).xattr().create_path().expand_macro(),
                    mutate_in_spec::upsert(TYPE, "remove").create_path().xattr(),
                };
                if (document.metadata()) {
                    if (document.metadata()->cas()) {
                        specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_CAS, document.metadata()->cas().value()).create_path().xattr());
                    }
                    if (document.metadata()->revid()) {
                        specs.emplace_back(
                          mutate_in_spec::upsert(PRE_TXN_REVID, document.metadata()->revid().value()).create_path().xattr());
                    }
                    if (document.metadata()->exptime()) {
                        specs.emplace_back(
                          mutate_in_spec::upsert(PRE_TXN_EXPTIME, document.metadata()->exptime().value()).create_path().xattr());
                    }
                }
                result res;
                wrap_collection_call(res, [&](result& r) {
                    r = collection->mutate_in(
                      document.id(),
                      specs,
                      wrap_option(mutate_in_options(), config_).access_deleted(document.links().is_deleted()).cas(document.cas()));
                });
                trace("removed doc {} CAS={}, rc={}", document.id(), res.cas, res.strerror());
                hooks_.after_staged_remove_complete(this, document.id());
                document.cas(res.cas);
                staged_mutations_->add(staged_mutation(document, "", staged_mutation_type::REMOVE));
            } catch (const client_error& e) {
                error_class ec = e.ec();
                switch (ec) {
                    case FAIL_EXPIRY:
                        expiry_overtime_mode_ = true;
                        throw transaction_operation_failed(ec, e.what()).expired();
                    case FAIL_DOC_NOT_FOUND:
                        throw transaction_operation_failed(ec, e.what()).retry();
                    case FAIL_DOC_ALREADY_EXISTS:
                    case FAIL_CAS_MISMATCH:
                        throw transaction_operation_failed(ec, e.what()).retry();
                    case FAIL_TRANSIENT:
                    case FAIL_AMBIGUOUS:
                        throw transaction_operation_failed(ec, e.what()).retry();
                    case FAIL_HARD:
                        throw transaction_operation_failed(ec, e.what()).no_rollback();
                    default:
                        throw transaction_operation_failed(ec, e.what());
                }
            }
        });
    }

    void attempt_context_impl::atr_commit()
    {
        try {
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + id() + ".");
            std::vector<mutate_in_spec> specs({
              mutate_in_spec::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::COMMITTED)).xattr(),
              mutate_in_spec::upsert(prefix + ATR_FIELD_START_COMMIT, "${Mutation.CAS}").xattr().expand_macro(),
            });
            error_if_expired_and_not_in_overtime(STAGE_ATR_COMMIT, {});
            hooks_.before_atr_commit(this);
            staged_mutations_->extract_to(prefix, specs);
            result res;
            wrap_collection_call(
              res, [&](result& r) { r = atr_collection_->mutate_in(atr_id_.value(), specs, wrap_option(mutate_in_options(), config_)); });
            hooks_.after_atr_commit(this);
            state(attempt_state::COMMITTED);
        } catch (const client_error& e) {
            error_class ec = e.ec();
            switch (ec) {
                case FAIL_EXPIRY:
                    expiry_overtime_mode_ = true;
                    throw transaction_operation_failed(ec, e.what()).expired();
                case FAIL_AMBIGUOUS:
                    debug("atr_commit got FAIL_AMBIGUOUS, resolving ambiguity...");
                    try {
                        return retry_op<void>([&]() { return atr_commit_ambiguity_resolution(); });
                    } catch (const retry_atr_commit& e) {
                        debug("ambiguity resolution will retry atr_commit");
                        throw retry_operation(e.what());
                    }
                case FAIL_TRANSIENT:
                    throw transaction_operation_failed(ec, e.what()).retry();
                case FAIL_HARD:
                    throw transaction_operation_failed(ec, e.what()).no_rollback();
                default:
                    error("failed to commit transaction {}, attempt {}, with error {}", transaction_id(), id(), e.what());
                    throw transaction_operation_failed(ec, e.what());
            }
        }
    }

    void attempt_context_impl::atr_commit_ambiguity_resolution()
    {
        try {
            error_if_expired_and_not_in_overtime(STAGE_ATR_COMMIT_AMBIGUITY_RESOLUTION, {});
            hooks_.before_atr_commit_ambiguity_resolution(this);
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + id() + ".");
            std::vector<lookup_in_spec> specs({ lookup_in_spec::get(prefix + ATR_FIELD_STATUS).xattr() });
            result res;
            wrap_collection_call(res, [&](result& r) { r = atr_collection_->lookup_in(atr_id_.value(), specs); });
            auto atr_status = attempt_state_value(res.values[0].value->get<std::string>());
            switch (atr_status) {
                case attempt_state::COMPLETED:
                    return;
                case attempt_state::ABORTED:
                case attempt_state::ROLLED_BACK:
                    // rolled back by another process?
                    throw transaction_operation_failed(FAIL_OTHER, "transaction rolled back externally").no_rollback();
                default:
                    // still pending - so we can safely retry
                    throw retry_atr_commit("atr still pending, retry atr_commit");
            }
        } catch (const client_error& e) {
            error_class ec = e.ec();
            switch (ec) {
                case FAIL_EXPIRY:
                    expiry_overtime_mode_ = true;
                    throw transaction_operation_failed(ec, e.what()).no_rollback().ambiguous();
                case FAIL_HARD:
                    throw transaction_operation_failed(ec, e.what()).no_rollback();
                case FAIL_TRANSIENT:
                case FAIL_OTHER:
                    throw retry_operation(e.what());
                case FAIL_PATH_NOT_FOUND:
                    throw transaction_operation_failed(FAIL_OTHER, "transaction rolled back externally").no_rollback();
                default:
                    throw transaction_operation_failed(ec, e.what()).no_rollback();
            }
        }
    }

    void attempt_context_impl::atr_complete()
    {
        try {
            result atr_res;
            hooks_.before_atr_complete(this);
            // if we have expired (and not in overtime mode), just raise the final error.
            error_if_expired_and_not_in_overtime(STAGE_ATR_COMPLETE, {});
            debug("removing attempt {} from atr", atr_id_.value());
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + id());
            wrap_collection_call(atr_res, [&](result& r) {
                r = atr_collection_->mutate_in(atr_id_.value(),
                                               { mutate_in_spec::upsert(prefix, nullptr).xattr(), mutate_in_spec::remove(prefix).xattr() },
                                               wrap_option(mutate_in_options(), config_));
            });
            hooks_.after_atr_complete(this);
            state(attempt_state::COMPLETED);
        } catch (const client_error& er) {
            error_class ec = er.ec();
            switch (ec) {
                case FAIL_HARD:
                    throw transaction_operation_failed(ec, er.what()).no_rollback().failed_post_commit();
                default:
                    info("ignoring error in atr_complete {}", er.what());
            }
        }
    }

    void attempt_context_impl::commit()
    {
        debug("commit {}", id());
        existing_error();
        try {
            check_expiry_pre_commit(STAGE_BEFORE_COMMIT, {});
        } catch (const client_error& e) {
            auto ec = e.ec();
            switch (ec) {
                case FAIL_EXPIRY:
                    throw transaction_operation_failed(ec, e.what()).expired();
                default:
                    throw transaction_operation_failed(ec, e.what());
            }
        }
        if (atr_collection_ && atr_id_ && !is_done_) {
            retry_op_exp<void>([&]() { atr_commit(); });
            staged_mutations_->commit(*this);
            atr_complete();
            is_done_ = true;
        } else {
            // no mutation, no need to commit
            if (!is_done_) {
                debug("calling commit on attempt that has got no mutations, skipping");
                is_done_ = true;
                return;
            } else {
                // do not rollback or retry
                throw transaction_operation_failed(FAIL_OTHER, "calling commit on attempt that is already completed").no_rollback();
            }
        }
    }

    void attempt_context_impl::atr_abort()
    {
        try {
            error_if_expired_and_not_in_overtime(STAGE_ATR_ABORT, {});
            hooks_.before_atr_aborted(this);
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + id() + ".");
            std::vector<mutate_in_spec> specs({
              mutate_in_spec::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::ABORTED)).xattr(),
              mutate_in_spec::upsert(prefix + ATR_FIELD_TIMESTAMP_ROLLBACK_START, "${Mutation.CAS}").xattr().expand_macro(),
            });
            staged_mutations_->extract_to(prefix, specs);
            result res;
            wrap_collection_call(
              res, [&](result& r) { r = atr_collection_->mutate_in(atr_id_.value(), specs, wrap_option(mutate_in_options(), config_)); });
            state(attempt_state::ABORTED);
            hooks_.after_atr_aborted(this);
            debug("rollback completed atr abort phase");
        } catch (const client_error& e) {
            auto ec = e.ec();
            if (expiry_overtime_mode_) {
                debug("atr_abort got error {} while in overtime mode", e.what());
                throw transaction_operation_failed(FAIL_EXPIRY, std::string("expired in atr_abort with {} ") + e.what())
                  .no_rollback()
                  .expired();
            }
            debug("atr_abort got error {}", ec);
            switch (ec) {
                case FAIL_EXPIRY:
                    expiry_overtime_mode_ = true;
                    throw retry_operation("expired, setting overtime mode and retry atr_abort");
                case FAIL_PATH_NOT_FOUND:
                    throw transaction_operation_failed(ec, e.what()).no_rollback().cause(ACTIVE_TRANSACTION_RECORD_ENTRY_NOT_FOUND);
                case FAIL_DOC_NOT_FOUND:
                    throw transaction_operation_failed(ec, e.what()).no_rollback().cause(ACTIVE_TRANSACTION_RECORD_NOT_FOUND);
                case FAIL_ATR_FULL:
                    throw transaction_operation_failed(ec, e.what()).no_rollback().cause(ACTIVE_TRANSACTION_RECORD_FULL);
                case FAIL_HARD:
                    throw transaction_operation_failed(ec, e.what()).no_rollback();
                default:
                    throw retry_operation("retry atr_abort");
            }
        }
    }

    void attempt_context_impl::atr_rollback_complete()
    {
        try {
            error_if_expired_and_not_in_overtime(STAGE_ATR_ROLLBACK_COMPLETE, boost::none);
            hooks_.before_atr_rolled_back(this);
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + id());
            result atr_res;
            wrap_collection_call(atr_res, [&](result& r) {
                r = atr_collection_->mutate_in(atr_id_.value(),
                                               { mutate_in_spec::upsert(prefix, nullptr).xattr(), mutate_in_spec::remove(prefix).xattr() },
                                               wrap_option(mutate_in_options(), config_));
            });
            state(attempt_state::ROLLED_BACK);
            hooks_.after_atr_rolled_back(this);
            is_done_ = true;

        } catch (const client_error& e) {
            auto ec = e.ec();
            if (expiry_overtime_mode_) {
                debug("atr_rollback_complete error while in overtime mode {}", e.what());
                throw transaction_operation_failed(FAIL_EXPIRY, std::string("expired in atr_rollback_complete with {} ") + e.what())
                  .no_rollback()
                  .expired();
            }
            debug("atr_rollback_complete got error {}", ec);
            switch (ec) {
                case FAIL_DOC_NOT_FOUND:
                case FAIL_PATH_NOT_FOUND:
                    debug("atr {} not found, ignoring", atr_id_.value());
                    is_done_ = true;
                    break;
                case FAIL_ATR_FULL:
                    debug("atr {} full!", atr_id_);
                    throw retry_operation(e.what());
                case FAIL_HARD:
                    throw transaction_operation_failed(ec, e.what()).no_rollback();
                case FAIL_EXPIRY:
                    debug("timed out writing atr {}", atr_id_);
                    throw transaction_operation_failed(ec, e.what()).no_rollback().expired();
                default:
                    debug("retrying atr_rollback_complete");
                    throw retry_operation(e.what());
            }
        }
    }

    void attempt_context_impl::rollback()
    {
        debug("rolling back");
        // check for expiry
        check_expiry_during_commit_or_rollback(STAGE_ROLLBACK, boost::none);
        if (!atr_id_ || !atr_collection_ || state() == attempt_state::NOT_STARTED) {
            // TODO: check this, but if we try to rollback an empty txn, we should
            // prevent a subsequent commit
            debug("rollback called on txn with no mutations");
            is_done_ = true;
            return;
        }
        if (is_done()) {
            std::string msg("Transaction already done, cannot rollback");
            error(msg);
            // need to raise a FAIL_OTHER which is not retryable or rollback-able
            throw transaction_operation_failed(FAIL_OTHER, msg).no_rollback();
        }
        try {
            // (1) atr_abort
            retry_op_exp<void>([&] { atr_abort(); });
            // (2) rollback staged mutations
            staged_mutations_->rollback(*this);
            debug("rollback completed unstaging docs");

            // (3) atr_rollback
            retry_op_exp<void>([&] { atr_rollback_complete(); });
        } catch (const client_error& e) {
            error_class ec = e.ec();
            error("rollback transaction {}, attempt {} fail with error {}", transaction_id(), id(), e.what());
            if (ec == FAIL_HARD) {
                throw transaction_operation_failed(ec, e.what()).no_rollback();
            }
        }
    }

    bool attempt_context_impl::has_expired_client_side(std::string place, boost::optional<const std::string> doc_id)
    {
        bool over = overall_.has_expired_client_side(config_);
        bool hook = hooks_.has_expired_client_side(this, place, doc_id);
        if (over) {
            debug("{} expired in {}", id(), place);
        }
        if (hook) {
            debug("{} fake expiry in {}", id(), place);
        }
        return over || hook;
    }

    void attempt_context_impl::check_expiry_pre_commit(std::string stage, boost::optional<const std::string> doc_id)
    {
        if (has_expired_client_side(stage, std::move(doc_id))) {
            debug("{} has expired in stage {}, entering expiry-overtime mode - will make one attempt to rollback", id(), stage);

            // [EXP-ROLLBACK] Combo of setting this mode and throwing AttemptExpired will result in a attempt to rollback, which will
            // ignore expiries, and bail out if anything fails
            expiry_overtime_mode_ = true;
            throw attempt_expired(std::string("Attempt has expired in stage ") + stage);
        }
    }

    void attempt_context_impl::error_if_expired_and_not_in_overtime(const std::string& stage, boost::optional<const std::string> doc_id)
    {
        if (expiry_overtime_mode_) {
            debug("not doing expired check in {} as already in expiry-overtime", stage);
            return;
        }
        if (has_expired_client_side(stage, std::move(doc_id))) {
            debug("expired in {}", stage);
            throw attempt_expired(std::string("Expired in ") + stage);
        }
    }

    void attempt_context_impl::check_expiry_during_commit_or_rollback(const std::string& stage, boost::optional<const std::string> doc_id)
    {
        // [EXP-COMMIT-OVERTIME]
        if (!expiry_overtime_mode_) {
            if (has_expired_client_side(stage, std::move(doc_id))) {
                debug("{} has expired in stage {}, entering expiry-overtime mode (one attempt to complete commit)", id(), stage);
                expiry_overtime_mode_ = true;
            }
        } else {
            debug("{} ignoring expiry in stage {}  as in expiry-overtime mode", id(), stage);
        }
    }

    void attempt_context_impl::set_atr_pending_if_first_mutation(std::shared_ptr<collection> collection)
    {
        if (staged_mutations_->empty()) {
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + id() + ".");
            if (!atr_id_) {
                throw transaction_operation_failed(FAIL_OTHER, std::string("ATR ID is not initialized"));
            }
            result res;
            try {
                error_if_expired_and_not_in_overtime(STAGE_ATR_PENDING, {});
                hooks_.before_atr_pending(this);
                debug("updating atr {}", atr_id_.value());
                wrap_collection_call(res, [&](result& r) {
                    r = atr_collection_->mutate_in(

                      atr_id_.value(),
                      { mutate_in_spec::insert(prefix + ATR_FIELD_TRANSACTION_ID, overall_.transaction_id()).xattr().create_path(),
                        mutate_in_spec::insert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::PENDING)).xattr().create_path(),
                        mutate_in_spec::insert(prefix + ATR_FIELD_START_TIMESTAMP, mutate_in_macro::CAS).xattr().expand_macro(),
                        mutate_in_spec::insert(prefix + ATR_FIELD_EXPIRES_AFTER_MSECS,
                                               std::chrono::duration_cast<std::chrono::milliseconds>(config_.expiration_time()).count())
                          .xattr() },
                      wrap_option(mutate_in_options(), config_).store_semantics(couchbase::subdoc_store_semantics::upsert));
                });
                debug("set ATR {}/{}/{} to Pending, got CAS (start time) {}",
                      collection->bucket_name(),
                      collection->name(),
                      atr_id_.value(),
                      res.cas);
                hooks_.after_atr_pending(this);
                state(attempt_state::PENDING);
            } catch (const client_error& e) {
                debug("caught {}, ec={}", e.what(), e.ec());
                if (expiry_overtime_mode_) {
                    throw transaction_operation_failed(FAIL_EXPIRY, e.what()).no_rollback().expired();
                }
                error_class ec = e.ec();
                switch (ec) {
                    case FAIL_EXPIRY:
                        expiry_overtime_mode_ = true;
                        // this should trigger rollback (unlike the above when already in overtime mode)
                        throw transaction_operation_failed(ec, e.what()).expired();
                    case FAIL_ATR_FULL:
                        throw transaction_operation_failed(ec, e.what());
                    case FAIL_PATH_ALREADY_EXISTS:
                        // assuming this got resolved, moving on as if ok
                        break;
                    case FAIL_AMBIGUOUS:
                        // Retry just this
                        overall_.retry_delay(config_);
                        return set_atr_pending_if_first_mutation(collection);
                    case FAIL_TRANSIENT:
                        // Retry txn
                        throw transaction_operation_failed(ec, e.what()).retry();
                    case FAIL_HARD:
                        throw transaction_operation_failed(ec, e.what()).no_rollback();
                    default:
                        throw transaction_operation_failed(ec, e.what());
                }
            }
        }
    }

    staged_mutation* attempt_context_impl::check_for_own_write(std::shared_ptr<collection> collection, const std::string& id)
    {
        staged_mutation* own_replace = staged_mutations_->find_replace(collection, id);
        if (own_replace) {
            return own_replace;
        }
        staged_mutation* own_insert = staged_mutations_->find_insert(collection, id);
        if (own_insert) {
            return own_insert;
        }
        return nullptr;
    }

    void attempt_context_impl::check_if_done()
    {
        if (is_done_) {
            throw transaction_operation_failed(FAIL_OTHER, "Cannot perform operations after transaction has been committed or rolled back")
              .no_rollback();
        }
    }

    boost::optional<transaction_get_result> attempt_context_impl::do_get(std::shared_ptr<collection> collection, const std::string& id)
    {
        try {
            check_if_done();
            check_expiry_pre_commit(STAGE_GET, id);

            staged_mutation* own_write = check_for_own_write(collection, id);
            if (own_write) {
                debug("found own-write of mutated doc {}", id);
                return transaction_get_result::create_from(own_write->doc(), own_write->content<const nlohmann::json&>());
            }
            staged_mutation* own_remove = staged_mutations_->find_remove(collection, id);
            if (own_remove) {
                debug("found own-write of removed doc {}", id);
                return {};
            }

            hooks_.before_doc_get(this, id);

            auto doc_res = get_doc(collection, id);
            if (!doc_res) {
                return {};
            }
            auto doc = doc_res->first;
            auto get_doc_res = doc_res->second;
            if (doc.links().is_document_in_transaction()) {
                debug("doc {} in transaction", doc);
                boost::optional<active_transaction_record> atr =
                  active_transaction_record::get_atr(collection, doc.links().atr_id().value());
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
                    if (entry) {
                        if (doc.links().staged_attempt_id() && entry->attempt_id() == this->id()) {
                            // Attempt is reading its own writes
                            // This is here as backup, it should be returned from the in-memory cache instead
                            content = doc.links().staged_content<nlohmann::json>();
                        } else {
                            forward_compat::check(forward_compat_stage::GETS_READING_ATR, entry->forward_compat());
                            switch (entry->state()) {
                                case attempt_state::COMMITTED:
                                    if (doc.links().is_document_being_removed()) {
                                        ignore_doc = true;
                                    } else {
                                        content = doc.links().staged_content<nlohmann::json>();
                                    }
                                    break;
                                default:
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
                        if (content.empty()) {
                            // This document is being inserted, so should not be visible yet
                            ignore_doc = true;
                        }
                    }
                    if (ignore_doc) {
                        return {};
                    } else {
                        return transaction_get_result::create_from(doc, content);
                    }
                } else {
                    // failed to get the ATR
                    if (doc.content<nlohmann::json>().empty()) {
                        // this document is being inserted, so should not be visible yet
                        return {};
                    } else {
                        return doc;
                    }
                }
            } else {
                if (get_doc_res.is_deleted) {
                    debug("doc not in txn, and is_deleted, so not returning it.");
                    // doc has been deleted, not in txn, so don't return it
                    return {};
                }
            }
            return doc;
        } catch (const client_error& e) {
            error_class ec = e.ec();
            switch (ec) {
                case FAIL_EXPIRY:
                    throw transaction_operation_failed(ec, e.what()).expired();
                case FAIL_DOC_NOT_FOUND:
                    return {};
                case FAIL_TRANSIENT:
                    throw transaction_operation_failed(ec, e.what()).retry();
                case FAIL_HARD:
                    throw transaction_operation_failed(ec, e.what()).no_rollback();
                default: {
                    std::string what(fmt::format("got error while getting doc {}: {}", id, e.what()));
                    throw transaction_operation_failed(FAIL_OTHER, what);
                }
            }
        }
    }

    boost::optional<std::pair<transaction_get_result, couchbase::result>> attempt_context_impl::get_doc(
      std::shared_ptr<couchbase::collection> collection,
      const std::string& id)
    {
        try {
            result res;
            wrap_collection_call(res, [&](result& r) {
                r = collection->lookup_in(id,
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
                                            lookup_in_spec::get(CRC32_OF_STAGING).xattr(),
                                            lookup_in_spec::get(FORWARD_COMPAT).xattr(),
                                            lookup_in_spec::fulldoc_get() },
                                          lookup_in_options().access_deleted(true));
            });
            return std::pair<transaction_get_result, result>(transaction_get_result::create_from(*collection, id, res), res);
        } catch (const client_error& e) {
            if (e.ec() == FAIL_DOC_NOT_FOUND) {
                return boost::none;
            }
            if (e.ec() == FAIL_PATH_NOT_FOUND) {
                return std::pair<transaction_get_result, result>(transaction_get_result::create_from(*collection, id, *e.res()), *e.res());
            }
            throw;
        }
    }

    transaction_get_result attempt_context_impl::create_staged_insert(std::shared_ptr<collection> collection,
                                                                      const std::string& id,
                                                                      const nlohmann::json& content,
                                                                      uint64_t& cas)
    {
        try {
            error_if_expired_and_not_in_overtime(STAGE_CREATE_STAGED_INSERT, id);
            hooks_.before_staged_insert(this, id);
            debug("about to insert staged doc {} with cas {}", id, cas);
            result res;
            wrap_collection_call(res, [&](result& r) {
                r = collection->mutate_in(
                  id,
                  {
                    mutate_in_spec::upsert(TRANSACTION_ID, overall_.transaction_id()).xattr().create_path(),
                    mutate_in_spec::upsert(ATTEMPT_ID, this->id()).create_path().xattr(),
                    mutate_in_spec::upsert(ATR_ID, atr_id_.value()).create_path().xattr(),
                    mutate_in_spec::upsert(STAGED_DATA, content).create_path().xattr(),
                    mutate_in_spec::upsert(ATR_BUCKET_NAME, collection->bucket_name()).create_path().xattr(),
                    mutate_in_spec::upsert(ATR_COLL_NAME, collection->scope() + "." + collection->name()).xattr().create_path(),
                    mutate_in_spec::upsert(TYPE, "insert").create_path().xattr(),
                    mutate_in_spec::upsert(CRC32_OF_STAGING, mutate_in_macro::VALUE_CRC_32C).create_path().xattr().expand_macro(),
                  },
                  wrap_option(mutate_in_options(), config_).access_deleted(true).create_as_deleted(true).cas(cas));
            });
            debug("inserted doc {} CAS={}, rc={}", id, res.cas, res.strerror());
            hooks_.after_staged_insert_complete(this, id);

            // TODO: clean this up (do most of this in transactions_document(...))
            transaction_links links(atr_id_,
                                    collection->bucket_name(),
                                    collection->scope(),
                                    collection->name(),
                                    overall_.transaction_id(),
                                    this->id(),
                                    nlohmann::json(content),
                                    boost::none,
                                    boost::none,
                                    boost::none,
                                    boost::none,
                                    std::string("insert"),
                                    boost::none,
                                    true);
            transaction_get_result out(id, content, res.cas, *collection, links, boost::none);
            staged_mutations_->add(staged_mutation(out, content, staged_mutation_type::INSERT));
            return out;
        } catch (const client_error& e) {
            error_class ec = e.ec();
            if (expiry_overtime_mode_) {
                throw transaction_operation_failed(FAIL_EXPIRY, "attempt timed out").expired();
            }
            switch (ec) {
                case FAIL_EXPIRY:
                    expiry_overtime_mode_ = true;
                    throw transaction_operation_failed(ec, "attempt timed-out").expired();
                case FAIL_TRANSIENT:
                    throw transaction_operation_failed(ec, "transient error in insert").retry();
                case FAIL_AMBIGUOUS:
                    throw retry_operation("FAIL_AMBIGUOUS in create_staged_insert");
                case FAIL_DOC_ALREADY_EXISTS:
                case FAIL_CAS_MISMATCH:
                    // special handling for doc already existing
                    try {
                        debug("found existing doc {}, may still be able to insert", id);
                        hooks_.before_get_doc_in_exists_during_staged_insert(this, id);
                        auto get_document = get_doc(collection, id);
                        if (get_document) {
                            auto doc = get_document->first;
                            auto get_res = get_document->second;
                            debug("document {} exists, is_in_transaction {}, is_deleted {} ",
                                  doc.id(),
                                  doc.links().is_document_in_transaction(),
                                  get_res.is_deleted);
                            forward_compat::check(forward_compat_stage::WWC_INSERTING_GET, doc.links().forward_compat());
                            if (!doc.links().is_document_in_transaction() && get_res.is_deleted) {
                                // it is just a deleted doc, so we are ok.  Lets try again, but with the cas
                                debug("doc was deleted, retrying with cas {}", doc.cas());
                                cas = doc.cas();
                                throw retry_operation("create staged insert found existing deleted doc, retrying");
                            }
                            if (!doc.links().is_document_in_transaction()) {
                                // doc was inserted outside txn elsewhere
                                throw transaction_operation_failed(FAIL_DOC_ALREADY_EXISTS, "document already exists");
                            }
                            // CBD-3787 - Only a staged insert is ok to overwrite
                            if (doc.links().op() && *doc.links().op() != "insert") {
                                throw transaction_operation_failed(FAIL_DOC_ALREADY_EXISTS, "doc exists, not a staged insert")
                                  .cause(DOCUMENT_EXISTS_EXCEPTION);
                            }
                            check_and_handle_blocking_transactions(doc, forward_compat_stage::WWC_INSERTING);
                            // if the check didn't throw, we can retry staging with cas
                            debug("doc ok to overwrite, retrying with cas {}", doc.cas());
                            cas = doc.cas();
                            throw retry_operation("create staged insert found existing non-blocking doc, retrying");
                        } else {
                            // no doc now, just retry entire txn
                            throw transaction_operation_failed(FAIL_DOC_NOT_FOUND,
                                                               "insert failed as the doc existed, but now seems to not exist")
                              .retry();
                        }
                    } catch (const transaction_operation_failed& get_err) {
                        switch (get_err.ec()) {
                            case FAIL_TRANSIENT:
                            case FAIL_PATH_NOT_FOUND:
                                debug("transient error trying to get doc in insert - retrying txn");
                                throw transaction_operation_failed(get_err.ec(), "error handling found doc in insert").retry();
                            default:
                                throw;
                        }
                    }
                case FAIL_OTHER:
                    throw transaction_operation_failed(ec, e.what());
                case FAIL_HARD:
                    throw transaction_operation_failed(ec, e.what()).no_rollback();
                default:
                    throw transaction_operation_failed(FAIL_OTHER, e.what()).retry();
            }
        }
    }

} // namespace transactions
} // namespace couchbase
