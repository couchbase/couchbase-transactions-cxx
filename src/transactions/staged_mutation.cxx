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

#include "staged_mutation.hxx"
#include "attempt_context_impl.hxx"
#include "couchbase/transactions/internal/transaction_fields.hxx"
#include "couchbase/transactions/internal/utils.hxx"
#include "result.hxx"
#include <utility>

namespace tx = couchbase::transactions;

bool
tx::staged_mutation_queue::empty()
{
    std::lock_guard<std::mutex> lock(mutex_);
    return queue_.empty();
}

void
tx::staged_mutation_queue::add(const tx::staged_mutation& mutation)
{
    std::lock_guard<std::mutex> lock(mutex_);
    // Can only have one staged mutation per document.
    auto new_end = std::remove_if(
      queue_.begin(), queue_.end(), [&mutation](const staged_mutation& item) { return document_ids_equal(item.id(), mutation.id()); });
    queue_.erase(new_end, queue_.end());
    queue_.push_back(mutation);
}

void
tx::staged_mutation_queue::extract_to(const std::string& prefix, couchbase::operations::mutate_in_request& req)
{
    std::lock_guard<std::mutex> lock(mutex_);
    nlohmann::json inserts;
    nlohmann::json replaces;
    nlohmann::json removes;

    for (auto& mutation : queue_) {
        nlohmann::json doc{ { ATR_FIELD_PER_DOC_ID, mutation.doc().id().key() },
                            { ATR_FIELD_PER_DOC_BUCKET, mutation.doc().id().bucket() },
                            { ATR_FIELD_PER_DOC_SCOPE, mutation.doc().id().scope() },
                            { ATR_FIELD_PER_DOC_COLLECTION, mutation.doc().id().collection() } };
        switch (mutation.type()) {
            case staged_mutation_type::INSERT:
                inserts.push_back(doc);
                break;
            case staged_mutation_type::REMOVE:
                removes.push_back(doc);
                break;
            case staged_mutation_type::REPLACE:
                replaces.push_back(doc);
                break;
        }
    }
    req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, prefix + ATR_FIELD_DOCS_INSERTED, inserts.dump());
    req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, prefix + ATR_FIELD_DOCS_REPLACED, replaces.dump());
    req.specs.add_spec(protocol::subdoc_opcode::dict_upsert, true, true, false, prefix + ATR_FIELD_DOCS_REMOVED, removes.dump());
}

void
tx::staged_mutation_queue::remove_any(const couchbase::document_id& id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    auto new_end =
      std::remove_if(queue_.begin(), queue_.end(), [id](const staged_mutation& item) { return document_ids_equal(item.id(), id); });
    queue_.erase(new_end, queue_.end());
}

tx::staged_mutation*
tx::staged_mutation_queue::find_any(const couchbase::document_id& id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (document_ids_equal(item.doc().id(), id)) {
            return &item;
        }
    }
    return nullptr;
}

tx::staged_mutation*
tx::staged_mutation_queue::find_replace(const couchbase::document_id& id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (item.type() == staged_mutation_type::REPLACE && document_ids_equal(item.doc().id(), id)) {
            return &item;
        }
    }
    return nullptr;
}

tx::staged_mutation*
tx::staged_mutation_queue::find_insert(const couchbase::document_id& id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (item.type() == staged_mutation_type::INSERT && document_ids_equal(item.doc().id(), id)) {
            return &item;
        }
    }
    return nullptr;
}

tx::staged_mutation*
tx::staged_mutation_queue::find_remove(const couchbase::document_id& id)
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (item.type() == staged_mutation_type::REMOVE && document_ids_equal(item.doc().id(), id)) {
            return &item;
        }
    }
    return nullptr;
}
void
tx::staged_mutation_queue::iterate(std::function<void(staged_mutation&)> op)
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        op(item);
    }
}

void
tx::staged_mutation_queue::commit(attempt_context_impl& ctx)
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        switch (item.type()) {
            case staged_mutation_type::REMOVE:
                remove_doc(ctx, item);
                break;
            case staged_mutation_type::INSERT:
            case staged_mutation_type::REPLACE:
                commit_doc(ctx, item);
                break;
        }
    }
}

void
tx::staged_mutation_queue::rollback(attempt_context_impl& ctx)
{
    std::lock_guard<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        switch (item.type()) {
            case staged_mutation_type::INSERT:
                retry_op_exp<void>([&]() { rollback_insert(ctx, item); });
                break;
            case staged_mutation_type::REMOVE:
            case staged_mutation_type::REPLACE:
                retry_op_exp<void>([&]() { rollback_remove_or_replace(ctx, item); });
                break;
        }
    }
}

void
tx::staged_mutation_queue::rollback_insert(attempt_context_impl& ctx, staged_mutation& item)
{
    try {
        ctx.trace("rolling back staged insert for {} with cas {}", item.doc().id(), item.doc().cas());
        auto ec = ctx.error_if_expired_and_not_in_overtime(STAGE_DELETE_INSERTED, item.doc().id().key());
        if (ec) {
            throw client_error(*ec, "expired in rollback and not in overtime mode");
        }
        ec = ctx.hooks_.before_rollback_delete_inserted(&ctx, item.doc().id().key());
        if (ec) {
            throw client_error(*ec, "before_rollback_delete_insert hook threw error");
        }
        couchbase::operations::mutate_in_request req{ item.doc().id() };
        req.specs.add_spec(protocol::subdoc_opcode::remove, true, TRANSACTION_INTERFACE_PREFIX_ONLY);
        req.access_deleted = true;
        req.cas.value = item.doc().cas();
        wrap_durable_request(req, ctx.overall_.config());
        auto barrier = std::make_shared<std::promise<result>>();
        auto f = barrier->get_future();
        ctx.cluster_ref().execute(req, [barrier](couchbase::operations::mutate_in_response resp) {
            barrier->set_value(result::create_from_subdoc_response(resp));
        });
        auto res = wrap_operation_future(f);
        ctx.trace("rollback result {}", res);
        ec = ctx.hooks_.after_rollback_delete_inserted(&ctx, item.doc().id().key());
        if (ec) {
            throw client_error(*ec, "after_rollback_delete_insert hook threw error");
        }
    } catch (const client_error& e) {
        auto ec = e.ec();
        if (ctx.expiry_overtime_mode_.load()) {
            ctx.trace("rollback_insert for {} error while in overtime mode {}", item.doc().id(), e.what());
            throw transaction_operation_failed(FAIL_EXPIRY, std::string("expired while rolling back insert with {} ") + e.what())
              .no_rollback()
              .expired();
        }
        switch (ec) {
            case FAIL_HARD:
            case FAIL_CAS_MISMATCH:
                throw transaction_operation_failed(ec, e.what()).no_rollback();
            case FAIL_EXPIRY:
                ctx.expiry_overtime_mode_ = true;
                ctx.trace("rollback_insert in expiry overtime mode, retrying...");
                throw retry_operation("retry rollback_insert");
            case FAIL_DOC_NOT_FOUND:
            case FAIL_PATH_NOT_FOUND:
                // already cleaned up?
                return;
            default:
                throw retry_operation("retry rollback insert");
        }
    }
}

void
tx::staged_mutation_queue::rollback_remove_or_replace(attempt_context_impl& ctx, staged_mutation& item)
{
    try {
        ctx.trace("rolling back staged remove/replace for {} with cas {}", item.doc().id(), item.doc().cas());
        auto ec = ctx.error_if_expired_and_not_in_overtime(STAGE_ROLLBACK_DOC, item.doc().id().key());
        if (ec) {
            throw client_error(*ec, "expired in rollback_remove_or_replace and not in expiry overtime");
        }
        ec = ctx.hooks_.before_doc_rolled_back(&ctx, item.doc().id().key());
        if (ec) {
            throw client_error(*ec, "before_doc_rolled_back hook threw error");
        }
        couchbase::operations::mutate_in_request req{ item.doc().id() };
        req.specs.add_spec(protocol::subdoc_opcode::remove, true, TRANSACTION_INTERFACE_PREFIX_ONLY);
        req.cas.value = item.doc().cas();
        wrap_durable_request(req, ctx.overall_.config());
        auto barrier = std::make_shared<std::promise<result>>();
        auto f = barrier->get_future();
        ctx.cluster_ref().execute(req, [barrier](couchbase::operations::mutate_in_response resp) {
            barrier->set_value(result::create_from_subdoc_response(resp));
        });
        auto res = wrap_operation_future(f);
        ctx.trace("rollback result {}", res);
        ec = ctx.hooks_.after_rollback_replace_or_remove(&ctx, item.doc().id().key());
        if (ec) {
            throw client_error(*ec, "after_rollback_replace_or_remove hook threw error");
        }
    } catch (const client_error& e) {
        auto ec = e.ec();
        if (ctx.expiry_overtime_mode_.load()) {
            throw transaction_operation_failed(FAIL_EXPIRY, std::string("expired while handling ") + e.what()).no_rollback();
        }
        switch (ec) {
            case FAIL_HARD:
            case FAIL_DOC_NOT_FOUND:
            case FAIL_CAS_MISMATCH:
                throw transaction_operation_failed(ec, e.what()).no_rollback();
            case FAIL_EXPIRY:
                ctx.expiry_overtime_mode_ = true;
                ctx.trace("setting expiry overtime mode in {}", STAGE_ROLLBACK_DOC);
                throw retry_operation("retry rollback_remove_or_replace");
            case FAIL_PATH_NOT_FOUND:
                // already cleaned up?
                return;
            default:
                throw retry_operation("retry rollback_remove_or_replace");
        }
    }
}
void
tx::staged_mutation_queue::commit_doc(attempt_context_impl& ctx, staged_mutation& item, bool ambiguity_resolution_mode, bool cas_zero_mode)
{
    retry_op<void>([&] {
        ctx.trace(
          "commit doc {}, cas_zero_mode {}, ambiguity_resolution_mode {}", item.doc().id(), cas_zero_mode, ambiguity_resolution_mode);
        try {
            ctx.check_expiry_during_commit_or_rollback(STAGE_COMMIT_DOC, std::optional<const std::string>(item.doc().id().key()));
            auto ec = ctx.hooks_.before_doc_committed(&ctx, item.doc().id().key());
            if (ec) {
                throw client_error(*ec, "before_doc_committed hook threw error");
            }

            // move staged content into doc
            ctx.trace("commit doc id {}, content {}, cas {}", item.doc().id(), item.content(), item.doc().cas());

            result res;
            if (item.type() == staged_mutation_type::INSERT && !cas_zero_mode) {
                couchbase::operations::insert_request req{ item.doc().id() };
                req.value = item.doc().content<nlohmann::json>().dump();
                wrap_durable_request(req, ctx.overall_.config());
                auto barrier = std::make_shared<std::promise<result>>();
                auto f = barrier->get_future();
                ctx.cluster_ref().execute(req, [barrier](couchbase::operations::insert_response resp) {
                    barrier->set_value(result::create_from_mutation_response(resp));
                });
                res = wrap_operation_future(f);
            } else {
                couchbase::operations::mutate_in_request req{ item.doc().id() };
                req.specs.add_spec(protocol::subdoc_opcode::remove, true, TRANSACTION_INTERFACE_PREFIX_ONLY);
                req.specs.add_spec(protocol::subdoc_opcode::set_doc, false, false, false, "", item.content());
                req.store_semantics = protocol::mutate_in_request_body::store_semantics_type::replace;
                req.cas.value = cas_zero_mode ? 0 : item.doc().cas();
                wrap_durable_request(req, ctx.overall_.config());
                auto barrier = std::make_shared<std::promise<result>>();
                auto f = barrier->get_future();
                ctx.cluster_ref().execute(req, [barrier](couchbase::operations::mutate_in_response resp) {
                    barrier->set_value(result::create_from_subdoc_response(resp));
                });
                res = wrap_operation_future(f);
            }
            ctx.trace("commit doc result {}", res);
            // TODO: mutation tokens
            ec = ctx.hooks_.after_doc_committed_before_saving_cas(&ctx, item.doc().id().key());
            if (ec) {
                throw client_error(*ec, "after_doc_committed_before_saving_cas threw error");
            }
            item.doc().cas(res.cas);
            ec = ctx.hooks_.after_doc_committed(&ctx, item.doc().id().key());
            if (ec) {
                throw client_error(*ec, "after_doc_committed threw error");
            }
        } catch (const client_error& e) {
            error_class ec = e.ec();
            if (ctx.expiry_overtime_mode_.load()) {
                throw transaction_operation_failed(FAIL_EXPIRY, "expired during commit").no_rollback().failed_post_commit();
            }
            switch (ec) {
                case FAIL_AMBIGUOUS:
                    ambiguity_resolution_mode = true;
                    throw retry_operation("FAIL_AMBIGUOUS in commit_doc");
                case FAIL_CAS_MISMATCH:
                case FAIL_DOC_ALREADY_EXISTS:
                    if (ambiguity_resolution_mode) {
                        throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
                    }
                    ambiguity_resolution_mode = true;
                    cas_zero_mode = true;
                    throw retry_operation("FAIL_DOC_ALREADY_EXISTS in commit_doc");
                default:
                    throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
            }
        }
    });
}

void
tx::staged_mutation_queue::remove_doc(attempt_context_impl& ctx, staged_mutation& item)
{
    retry_op<void>([&] {
        try {
            ctx.check_expiry_during_commit_or_rollback(STAGE_REMOVE_DOC, std::optional<const std::string>(item.doc().id().key()));
            auto ec = ctx.hooks_.before_doc_removed(&ctx, item.doc().id().key());
            if (ec) {
                throw client_error(*ec, "before_doc_removed hook threw error");
            }
            couchbase::operations::remove_request req{ item.doc().id() };
            wrap_durable_request(req, ctx.overall_.config());
            auto barrier = std::make_shared<std::promise<result>>();
            auto f = barrier->get_future();
            ctx.cluster_ref().execute(req, [barrier](couchbase::operations::remove_response resp) {
                barrier->set_value(result::create_from_mutation_response(resp));
            });
            wrap_operation_future(f);
            ec = ctx.hooks_.after_doc_removed_pre_retry(&ctx, item.doc().id().key());
            if (ec) {
                throw client_error(*ec, "after_doc_removed_pre_retry threw error");
            }
        } catch (const client_error& e) {
            error_class ec = e.ec();
            if (ctx.expiry_overtime_mode_.load()) {
                throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
            }
            switch (ec) {
                case FAIL_AMBIGUOUS:
                    throw retry_operation("remove_doc got FAIL_AMBIGUOUS");
                default:
                    throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
            }
        }
    });
}
