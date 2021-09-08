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
    cluster& attempt_context_impl::cluster_ref()
    {
        return parent_->cluster_ref();
    }

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
      , is_done_(false)
      , staged_mutations_(new staged_mutation_queue())
      , hooks_(config.attempt_context_hooks())
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
                          *doc.links().staged_attempt_id());
                }
            }
        }
    }

    transaction_get_result attempt_context_impl::get(const couchbase::document_id& id)
    {
        return cache_error<transaction_get_result>([&]() {
            auto result = get_optional(id);
            if (result) {
                trace("get returning {}", *result);
                return *result;
            }
            error("Document with id {} not found", id);
            throw transaction_operation_failed(FAIL_DOC_NOT_FOUND, "Document not found");
        });
    }

    std::optional<transaction_get_result> attempt_context_impl::get_optional(const couchbase::document_id& id)
    {
        return cache_error<std::optional<transaction_get_result>>([&]() {
            auto retval = do_get(id);
            hooks_.after_get_complete(this, id.key());
            if (retval) {
                forward_compat::check(forward_compat_stage::GETS, retval->links().forward_compat());
            }
            return retval;
        });
    }

    couchbase::operations::mutate_in_request attempt_context_impl::create_staging_request(const transaction_get_result& document,
                                                                                          const std::string type)
    {
        couchbase::operations::mutate_in_request req{ document.id() };
        req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, TRANSACTION_ID, jsonify(overall_.transaction_id()));
        req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, ATTEMPT_ID, jsonify(id()));
        req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, STAGED_DATA, document.content<std::string>());
        req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, ATR_ID, jsonify(atr_id()));
        req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, ATR_BUCKET_NAME, jsonify(document.id().bucket()));
        req.specs.add_spec(
          protocol::subdoc_opcode::dict_upsert, true, true, false, ATR_COLL_NAME, jsonify(collection_spec_from_id(atr_id_.value())));
        req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, true, CRC32_OF_STAGING, mutate_in_macro::VALUE_CRC_32C);
        req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, TYPE, jsonify(type));

        if (document.metadata()) {
            if (document.metadata()->cas()) {
                req.specs.add_spec(
                  protocol::subdoc_opcode::dict_upsert, true, true, false, PRE_TXN_CAS, jsonify(document.metadata()->cas().value()));
            }
            if (document.metadata()->revid()) {
                req.specs.add_spec(
                  protocol::subdoc_opcode::dict_upsert, true, true, false, PRE_TXN_REVID, jsonify(document.metadata()->revid().value()));
            }
            if (document.metadata()->exptime()) {
                req.specs.add_spec(protocol::subdoc_opcode::dict_upsert,
                                   true,
                                   true,
                                   false,
                                   PRE_TXN_EXPTIME,
                                   jsonify(document.metadata()->exptime().value()));
            }
        }
        return wrap_durable_request(req, config_);
    }

    transaction_get_result attempt_context_impl::replace_raw(const transaction_get_result& document, const std::string& content)
    {
        return retry_op_exp<transaction_get_result>([&]() -> transaction_get_result {
            return cache_error<transaction_get_result>([&]() {
                try {
                    trace("replacing {} with {}", document, content);
                    check_if_done();
                    check_expiry_pre_commit(STAGE_REPLACE, document.id().key());
                    select_atr_if_needed(document.id());
                    check_and_handle_blocking_transactions(document, forward_compat_stage::WWC_REPLACING);
                    set_atr_pending_if_first_mutation(document.id());
                    auto req = create_staging_request(document, "replace");
                    hooks_.before_staged_replace(this, document.id().key());
                    trace("about to replace doc {} with cas {} in txn {}", document.id(), document.cas(), overall_.transaction_id());
                    auto barrier = std::make_shared<std::promise<result>>();
                    auto f = barrier->get_future();
                    parent_->cluster_ref().execute(req, [barrier](couchbase::operations::mutate_in_response resp) mutable {
                        barrier->set_value(result::create_from_subdoc_response(resp));
                    });
                    auto res = wrap_operation_future(f);
                    hooks_.after_staged_replace_complete(this, document.id().key());
                    transaction_get_result out = document;
                    out.cas(res.cas);
                    trace("replace staged content, result {}", res);
                    staged_mutation* existing_replace = staged_mutations_->find_replace(document.id());
                    staged_mutation* existing_insert = staged_mutations_->find_insert(document.id());
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

    transaction_get_result attempt_context_impl::insert_raw(const couchbase::document_id& id, const std::string& content)
    {
        return cache_error<transaction_get_result>([&]() {
            try {
                check_if_done();
                if (check_for_own_write(id)) {
                    throw transaction_operation_failed(FAIL_OTHER,
                                                       "cannot insert a document that has already been mutated in this transaction");
                }
                check_expiry_pre_commit(STAGE_INSERT, id.key());
                select_atr_if_needed(id);
                set_atr_pending_if_first_mutation(id);
                uint64_t cas = 0;
                return retry_op<transaction_get_result>([&]() -> transaction_get_result { return create_staged_insert(id, content, cas); });
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

    // Make thread-safe - atomic check then a lock and second check, perhaps.
    void attempt_context_impl::select_atr_if_needed(const couchbase::document_id& id)
    {
        if (!atr_id_) {
            size_t vbucket_id = 0;
            std::optional<const std::string> hook_atr = hooks_.random_atr_id_for_vbucket(this);
            if (hook_atr) {
                atr_id_ = { id.bucket(), "_default", "_default", *hook_atr };
            } else {
                vbucket_id = atr_ids::vbucket_for_key(id.key());
                atr_id_ = { id.bucket(), "_default", "_default", atr_ids::atr_id_for_vbucket(vbucket_id) };
            }
            overall_.atr_collection(collection_spec_from_id(id));
            overall_.atr_id(atr_id_->key());
            state(attempt_state::NOT_STARTED);
            trace("first mutated doc in transaction is \"{}\" on vbucket {}, so using atr \"{}\"", id, vbucket_id, atr_id_.value());
        }
    }

    void attempt_context_impl::check_atr_entry_for_blocking_document(const transaction_get_result& doc)
    {
        try {
            retry_op_exponential_backoff_timeout<void>(
              std::chrono::milliseconds(50), std::chrono::milliseconds(500), std::chrono::seconds(1), [&] {
                  try {
                      hooks_.before_check_atr_entry_for_blocking_doc(this, doc.id().key());
                      couchbase::document_id atr_id(doc.links().atr_bucket_name().value(),
                                                    doc.links().atr_scope_name().value(),
                                                    doc.links().atr_collection_name().value(),
                                                    doc.links().atr_id().value());
                      auto atr = active_transaction_record::get_atr(cluster_ref(), atr_id);
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

    void attempt_context_impl::remove(transaction_get_result& document)
    {
        return cache_error<void>([&]() {
            try {
                check_if_done();
                check_expiry_pre_commit(STAGE_REMOVE, document.id().key());
                if (staged_mutations_->find_insert(document.id())) {
                    error("cannot remove document {}, as it was inserted in this transaction", document.id());
                    throw transaction_operation_failed(FAIL_OTHER, "Cannot remove a document inserted in the same transaction");
                }
                trace("removing {}", document);
                check_and_handle_blocking_transactions(document, forward_compat_stage::WWC_REMOVING);
                select_atr_if_needed(document.id());

                set_atr_pending_if_first_mutation(document.id());

                hooks_.before_staged_remove(this, document.id().key());
                trace("about to remove remove doc {} with cas {}", document.id(), document.cas());
                auto req = create_staging_request(document, "remove");
                req.cas.value = document.cas();
                req.access_deleted = document.links().is_deleted();
                auto barrier = std::make_shared<std::promise<result>>();
                auto f = barrier->get_future();
                parent_->cluster_ref().execute(req, [barrier](couchbase::operations::mutate_in_response resp) mutable {
                    barrier->set_value(result::create_from_subdoc_response(resp));
                });
                auto res = wrap_operation_future(f);
                trace("removed doc {} CAS={}, rc={}", document.id(), res.cas, res.strerror());
                hooks_.after_staged_remove_complete(this, document.id().key());
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
            couchbase::operations::mutate_in_request req{ atr_id_.value() };
            req.specs.add_spec(protocol::subdoc_opcode::dict_upsert,
                               true,
                               false,
                               false,
                               prefix + ATR_FIELD_STATUS,
                               jsonify(attempt_state_name(attempt_state::COMMITTED)));
            req.specs.add_spec(
              protocol::subdoc_opcode::dict_upsert, true, false, true, prefix + ATR_FIELD_START_COMMIT, mutate_in_macro::CAS);
            wrap_durable_request(req, config_);
            error_if_expired_and_not_in_overtime(STAGE_ATR_COMMIT, {});
            hooks_.before_atr_commit(this);
            staged_mutations_->extract_to(prefix, req);
            auto barrier = std::make_shared<std::promise<result>>();
            auto f = barrier->get_future();
            trace("updating atr {}", req.id);
            parent_->cluster_ref().execute(req, [barrier](couchbase::operations::mutate_in_response resp) mutable {
                barrier->set_value(result::create_from_subdoc_response(resp));
            });
            auto res = wrap_operation_future(f);
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
            couchbase::operations::lookup_in_request req{ atr_id_.value() };
            req.specs.add_spec(protocol::subdoc_opcode::get, true, prefix + ATR_FIELD_STATUS);
            wrap_request(req, config_);
            auto barrier = std::make_shared<std::promise<result>>();
            auto f = barrier->get_future();
            parent_->cluster_ref().execute(req, [barrier](couchbase::operations::lookup_in_response resp) mutable {
                barrier->set_value(result::create_from_subdoc_response(resp));
            });
            auto res = wrap_operation_future(f);
            auto atr_status = attempt_state_value(res.values[0].content_as<std::string>());
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
            couchbase::operations::mutate_in_request req{ atr_id_.value() };
            req.specs.add_spec(protocol::subdoc_opcode::remove, true, prefix);
            wrap_durable_request(req, config_);
            auto barrier = std::make_shared<std::promise<result>>();
            auto f = barrier->get_future();
            parent_->cluster_ref().execute(req, [barrier](couchbase::operations::mutate_in_response resp) mutable {
                barrier->set_value(result::create_from_subdoc_response(resp));
            });
            wrap_operation_future(f);
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
        if (atr_id_ && !atr_id_->key().empty() && !is_done_) {
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
            couchbase::operations::mutate_in_request req{ atr_id_.value() };
            req.specs.add_spec(protocol::subdoc_opcode::dict_upsert,
                               true,
                               true,
                               false,
                               prefix + ATR_FIELD_STATUS,
                               jsonify(attempt_state_name(attempt_state::ABORTED)));
            req.specs.add_spec(
              protocol::subdoc_opcode::dict_upsert, true, true, true, prefix + ATR_FIELD_TIMESTAMP_ROLLBACK_START, mutate_in_macro::CAS);
            staged_mutations_->extract_to(prefix, req);
            wrap_durable_request(req, config_);
            auto barrier = std::make_shared<std::promise<result>>();
            auto f = barrier->get_future();
            parent_->cluster_ref().execute(req, [barrier](couchbase::operations::mutate_in_response resp) mutable {
                barrier->set_value(result::create_from_subdoc_response(resp));
            });
            wrap_operation_future(f);
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
            error_if_expired_and_not_in_overtime(STAGE_ATR_ROLLBACK_COMPLETE, std::nullopt);
            hooks_.before_atr_rolled_back(this);
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + id());
            couchbase::operations::mutate_in_request req{ atr_id_.value() };
            req.specs.add_spec(protocol::subdoc_opcode::remove, true, prefix);
            wrap_durable_request(req, config_);
            auto barrier = std::make_shared<std::promise<result>>();
            auto f = barrier->get_future();
            parent_->cluster_ref().execute(req, [barrier](couchbase::operations::mutate_in_response resp) mutable {
                barrier->set_value(result::create_from_subdoc_response(resp));
            });
            wrap_operation_future(f);
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
                    debug("atr {} not found, ignoring", atr_id_->key());
                    is_done_ = true;
                    break;
                case FAIL_ATR_FULL:
                    debug("atr {} full!", atr_id_->key());
                    throw retry_operation(e.what());
                case FAIL_HARD:
                    throw transaction_operation_failed(ec, e.what()).no_rollback();
                case FAIL_EXPIRY:
                    debug("timed out writing atr {}", atr_id_->key());
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
        check_expiry_during_commit_or_rollback(STAGE_ROLLBACK, std::nullopt);
        if (!atr_id_ || atr_id_->key().empty() || state() == attempt_state::NOT_STARTED) {
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

    bool attempt_context_impl::has_expired_client_side(std::string place, std::optional<const std::string> doc_id)
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

    void attempt_context_impl::check_expiry_pre_commit(std::string stage, std::optional<const std::string> doc_id)
    {
        if (has_expired_client_side(stage, std::move(doc_id))) {
            debug("{} has expired in stage {}, entering expiry-overtime mode - will make one attempt to rollback", id(), stage);

            // [EXP-ROLLBACK] Combo of setting this mode and throwing AttemptExpired will result in a attempt to rollback, which will
            // ignore expiries, and bail out if anything fails
            expiry_overtime_mode_ = true;
            throw attempt_expired(std::string("Attempt has expired in stage ") + stage);
        }
    }

    void attempt_context_impl::error_if_expired_and_not_in_overtime(const std::string& stage, std::optional<const std::string> doc_id)
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

    void attempt_context_impl::check_expiry_during_commit_or_rollback(const std::string& stage, std::optional<const std::string> doc_id)
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

    void attempt_context_impl::set_atr_pending_if_first_mutation(const couchbase::document_id& id)
    {
        if (staged_mutations_->empty()) {
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + this->id() + ".");
            if (!atr_id_) {
                throw transaction_operation_failed(FAIL_OTHER, std::string("ATR ID is not initialized"));
            }
            try {
                error_if_expired_and_not_in_overtime(STAGE_ATR_PENDING, {});
                hooks_.before_atr_pending(this);
                debug("updating atr {}", atr_id_.value());
                couchbase::operations::mutate_in_request req{ atr_id_.value() };

                req.specs.add_spec(protocol::subdoc_opcode::dict_add,
                                   true,
                                   true,
                                   false,
                                   prefix + ATR_FIELD_TRANSACTION_ID,
                                   jsonify(overall_.transaction_id()));
                req.specs.add_spec(protocol::subdoc_opcode::dict_add,
                                   true,
                                   true,
                                   false,
                                   prefix + ATR_FIELD_STATUS,
                                   jsonify(attempt_state_name(attempt_state::PENDING)));
                req.specs.add_spec(
                  protocol::subdoc_opcode::dict_add, true, true, true, prefix + ATR_FIELD_START_TIMESTAMP, mutate_in_macro::CAS);
                req.specs.add_spec(protocol::subdoc_opcode::dict_add,
                                   true,
                                   true,
                                   false,
                                   prefix + ATR_FIELD_EXPIRES_AFTER_MSECS,
                                   jsonify(std::chrono::duration_cast<std::chrono::milliseconds>(config_.expiration_time()).count()));
                req.store_semantics = protocol::mutate_in_request_body::store_semantics_type::upsert;
                // wrap_durable_request(req, config_);
                auto barrier = std::make_shared<std::promise<result>>();
                auto f = barrier->get_future();
                parent_->cluster_ref().execute(req, [barrier](couchbase::operations::mutate_in_response resp) mutable {
                    barrier->set_value(result::create_from_subdoc_response(resp));
                });
                auto res = wrap_operation_future(f);
                debug("set ATR {} to Pending, got CAS (start time) {}", atr_id_.value(), res.cas);
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
                        return set_atr_pending_if_first_mutation(id);
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

    staged_mutation* attempt_context_impl::check_for_own_write(const couchbase::document_id& id)
    {
        staged_mutation* own_replace = staged_mutations_->find_replace(id);
        if (own_replace) {
            return own_replace;
        }
        staged_mutation* own_insert = staged_mutations_->find_insert(id);
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

    std::optional<transaction_get_result> attempt_context_impl::do_get(const couchbase::document_id& id)
    {
        try {
            check_if_done();
            check_expiry_pre_commit(STAGE_GET, id.key());

            staged_mutation* own_write = check_for_own_write(id);
            if (own_write) {
                debug("found own-write of mutated doc {}", id);
                return transaction_get_result::create_from(own_write->doc(), own_write->content());
            }
            staged_mutation* own_remove = staged_mutations_->find_remove(id);
            if (own_remove) {
                debug("found own-write of removed doc {}", id);
                return {};
            }

            hooks_.before_doc_get(this, id.key());

            auto doc_res = get_doc(id);
            if (!doc_res) {
                return {};
            }
            auto doc = doc_res->first;
            auto get_doc_res = doc_res->second;
            if (doc.links().is_document_in_transaction()) {
                debug("doc {} in transaction", doc);
                couchbase::document_id doc_atr_id{ doc.links().atr_bucket_name().value(),
                                                   doc.links().atr_scope_name().value(),
                                                   doc.links().atr_collection_name().value(),
                                                   doc.links().atr_id().value() };
                std::optional<active_transaction_record> atr = active_transaction_record::get_atr(cluster_ref(), doc_atr_id);
                if (atr) {
                    active_transaction_record& atr_doc = atr.value();
                    std::optional<atr_entry> entry;
                    for (auto& e : atr_doc.entries()) {
                        if (doc.links().staged_attempt_id().value() == e.attempt_id()) {
                            entry.emplace(e);
                            break;
                        }
                    }
                    bool ignore_doc = false;
                    auto content = doc.content<std::string>();
                    if (entry) {
                        if (doc.links().staged_attempt_id() && entry->attempt_id() == this->id()) {
                            // Attempt is reading its own writes
                            // This is here as backup, it should be returned from the in-memory cache instead
                            content = doc.links().staged_content();
                        } else {
                            forward_compat::check(forward_compat_stage::GETS_READING_ATR, entry->forward_compat());
                            switch (entry->state()) {
                                case attempt_state::COMMITTED:
                                    if (doc.links().is_document_being_removed()) {
                                        ignore_doc = true;
                                    } else {
                                        content = doc.links().staged_content();
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
                    std::ostringstream stream;
                    stream << "got error while getting doc " << id.key() << ": " << e.what();
                    throw transaction_operation_failed(FAIL_OTHER, stream.str());
                }
            }
        } catch (const std::exception& ex) {
            std::ostringstream stream;
            stream << "got error while getting doc " << id.key() << ": " << ex.what();
            throw transaction_operation_failed(FAIL_OTHER, ex.what());
        }
    }

    std::optional<std::pair<transaction_get_result, result>> attempt_context_impl::get_doc(const couchbase::document_id& id)
    {
        try {
            couchbase::operations::lookup_in_request req{ id };
            req.specs.add_spec(protocol::subdoc_opcode::get, true, ATR_ID);
            req.specs.add_spec(protocol::subdoc_opcode::get, true, TRANSACTION_ID);
            req.specs.add_spec(protocol::subdoc_opcode::get, true, ATTEMPT_ID);
            req.specs.add_spec(protocol::subdoc_opcode::get, true, STAGED_DATA);
            req.specs.add_spec(protocol::subdoc_opcode::get, true, ATR_BUCKET_NAME);
            req.specs.add_spec(protocol::subdoc_opcode::get, true, ATR_COLL_NAME);
            req.specs.add_spec(protocol::subdoc_opcode::get, true, TRANSACTION_RESTORE_PREFIX_ONLY);
            req.specs.add_spec(protocol::subdoc_opcode::get, true, TYPE);
            req.specs.add_spec(protocol::subdoc_opcode::get, true, "$document");
            req.specs.add_spec(protocol::subdoc_opcode::get, true, CRC32_OF_STAGING);
            req.specs.add_spec(protocol::subdoc_opcode::get, true, FORWARD_COMPAT);
            req.specs.add_spec(protocol::subdoc_opcode::get_doc, false, "");
            req.access_deleted = true;
            wrap_request(req, config_);
            auto barrier = std::make_shared<std::promise<result>>();
            auto f = barrier->get_future();
            parent_->cluster_ref().execute(req, [barrier](couchbase::operations::lookup_in_response resp) mutable {
                barrier->set_value(result::create_from_subdoc_response(resp));
            });
            auto res = wrap_operation_future(f);
            return std::pair<transaction_get_result, result>(transaction_get_result::create_from(id, res), res);
        } catch (const client_error& e) {
            if (e.ec() == FAIL_DOC_NOT_FOUND) {
                return std::nullopt;
            }
            if (e.ec() == FAIL_PATH_NOT_FOUND) {
                return std::pair<transaction_get_result, result>(transaction_get_result::create_from(id, *e.res()), *e.res());
            }
            throw;
        }
    }

    transaction_get_result attempt_context_impl::create_staged_insert(const couchbase::document_id& id,
                                                                      const std::string& content,
                                                                      uint64_t& cas)
    {
        try {
            error_if_expired_and_not_in_overtime(STAGE_CREATE_STAGED_INSERT, id.key());
            hooks_.before_staged_insert(this, id.key());
            debug("about to insert staged doc {} with cas {}", id, cas);
            couchbase::operations::mutate_in_request req{ id };
            req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, TRANSACTION_ID, jsonify(overall_.transaction_id()));
            req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, ATTEMPT_ID, jsonify(this->id()));
            req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, ATR_ID, jsonify(atr_id()));
            req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, STAGED_DATA, content);
            req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, ATR_BUCKET_NAME, jsonify(id.bucket()));
            req.specs.add_spec(
              protocol::subdoc_opcode::dict_upsert, true, true, false, ATR_COLL_NAME, jsonify(collection_spec_from_id(id)));
            req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, TYPE, jsonify("insert"));
            req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, true, CRC32_OF_STAGING, mutate_in_macro::VALUE_CRC_32C);
            req.access_deleted = true;
            req.create_as_deleted = true;
            req.store_semantics = protocol::mutate_in_request_body::store_semantics_type::insert;
            req.cas.value = cas;
            wrap_durable_request(req, config_);
            auto barrier = std::make_shared<std::promise<result>>();
            auto f = barrier->get_future();
            parent_->cluster_ref().execute(req, [barrier](couchbase::operations::mutate_in_response resp) mutable {
                barrier->set_value(result::create_from_subdoc_response(resp));
            });
            auto res = wrap_operation_future(f);
            debug("inserted doc {} CAS={}, rc={}", id, res.cas, res.strerror());
            hooks_.after_staged_insert_complete(this, id.key());

            // TODO: clean this up (do most of this in transactions_document(...))
            transaction_links links(atr_id_->key(),
                                    id.bucket(),
                                    id.scope(),
                                    id.collection(),
                                    overall_.transaction_id(),
                                    this->id(),
                                    content,
                                    std::nullopt,
                                    std::nullopt,
                                    std::nullopt,
                                    std::nullopt,
                                    std::string("insert"),
                                    std::nullopt,
                                    true);
            transaction_get_result out(id, content, res.cas, links, std::nullopt);
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
                        hooks_.before_get_doc_in_exists_during_staged_insert(this, id.key());
                        auto get_document = get_doc(id);
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
