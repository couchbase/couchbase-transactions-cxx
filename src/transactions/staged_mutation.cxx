#include "staged_mutation.hxx"
#include "attempt_context_impl.hxx"
#include "transaction_fields.hxx"
#include "utils.hxx"
#include <couchbase/client/result.hxx>
#include <utility>

namespace tx = couchbase::transactions;

bool
tx::staged_mutation_queue::empty()
{
    std::unique_lock<std::mutex> lock(mutex_);
    return queue_.empty();
}

void
tx::staged_mutation_queue::add(const tx::staged_mutation& mutation)
{
    std::unique_lock<std::mutex> lock(mutex_);
    queue_.push_back(mutation);
}

void
tx::staged_mutation_queue::extract_to(const std::string& prefix, std::vector<couchbase::mutate_in_spec>& specs)
{
    std::unique_lock<std::mutex> lock(mutex_);
    std::vector<nlohmann::json> inserts;
    std::vector<nlohmann::json> replaces;
    std::vector<nlohmann::json> removes;

    for (auto& mutation : queue_) {
        nlohmann::json doc{ { ATR_FIELD_PER_DOC_ID, mutation.doc().id() },
                            { ATR_FIELD_PER_DOC_BUCKET, mutation.doc().collection_ref().bucket_name() },
                            { ATR_FIELD_PER_DOC_SCOPE, mutation.doc().collection_ref().scope() },
                            { ATR_FIELD_PER_DOC_COLLECTION, mutation.doc().collection_ref().name() } };
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
    specs.push_back(mutate_in_spec::upsert(prefix + ATR_FIELD_DOCS_INSERTED, inserts).xattr());
    specs.push_back(mutate_in_spec::upsert(prefix + ATR_FIELD_DOCS_REPLACED, replaces).xattr());
    specs.push_back(mutate_in_spec::upsert(prefix + ATR_FIELD_DOCS_REMOVED, removes).xattr());
}

tx::staged_mutation*
tx::staged_mutation_queue::find_replace(std::shared_ptr<couchbase::collection> collection, const std::string& id)
{
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (item.type() == staged_mutation_type::REPLACE && item.doc().id() == id &&
            item.doc().collection_ref().bucket_name() == collection->bucket_name() &&
            item.doc().collection_ref().scope() == collection->scope() && item.doc().collection_ref().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}

tx::staged_mutation*
tx::staged_mutation_queue::find_insert(std::shared_ptr<couchbase::collection> collection, const std::string& id)
{
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (item.type() == staged_mutation_type::INSERT && item.doc().id() == id &&
            item.doc().collection_ref().bucket_name() == collection->bucket_name() &&
            item.doc().collection_ref().scope() == collection->scope() && item.doc().collection_ref().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}

tx::staged_mutation*
tx::staged_mutation_queue::find_remove(std::shared_ptr<couchbase::collection> collection, const std::string& id)
{
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        if (item.type() == staged_mutation_type::REMOVE && item.doc().id() == id &&
            item.doc().collection_ref().bucket_name() == collection->bucket_name() &&
            item.doc().collection_ref().scope() == collection->scope() && item.doc().collection_ref().name() == collection->name()) {
            return &item;
        }
    }
    return nullptr;
}
void
tx::staged_mutation_queue::iterate(std::function<void(staged_mutation&)> op)
{
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        op(item);
    }
}

void
tx::staged_mutation_queue::commit(attempt_context_impl& ctx)
{
    std::unique_lock<std::mutex> lock(mutex_);
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
    std::unique_lock<std::mutex> lock(mutex_);
    for (auto& item : queue_) {
        switch (item.type()) {
            case staged_mutation_type::INSERT:
                retry_op<void>([&]() { rollback_insert(ctx, item); });
                break;
            case staged_mutation_type::REMOVE:
            case staged_mutation_type::REPLACE:
                retry_op<void>([&]() { rollback_remove_or_replace(ctx, item); });
                break;
        }
    }
}

void
tx::staged_mutation_queue::rollback_insert(attempt_context_impl& ctx, staged_mutation& item)
{
    try {
        ctx.trace("rolling back staged insert for {} with cas {}", item.doc().id(), item.doc().cas());
        ctx.error_if_expired_and_not_in_overtime(STAGE_DELETE_INSERTED, item.doc().id());
        ctx.hooks_.before_rollback_delete_inserted(&ctx, item.doc().id());
        std::vector<mutate_in_spec> specs({ mutate_in_spec::remove(TRANSACTION_INTERFACE_PREFIX_ONLY).xattr() });
        result res;
        tx::wrap_collection_call(res, [&](result& r) {
            r =
              item.doc().collection_ref().mutate_in(item.doc().id(), specs, mutate_in_options().access_deleted(true).cas(item.doc().cas()));
        });
        ctx.trace("rollback result {}", res);
        ctx.hooks_.after_rollback_delete_inserted(&ctx, item.doc().id());
    } catch (const client_error& e) {
        auto ec = e.ec();
        if (ctx.expiry_overtime_mode_) {
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
        ctx.error_if_expired_and_not_in_overtime(STAGE_ROLLBACK_DOC, item.doc().id());
        ctx.hooks_.before_doc_rolled_back(&ctx, item.doc().id());
        std::vector<mutate_in_spec> specs({ mutate_in_spec::remove(TRANSACTION_INTERFACE_PREFIX_ONLY).xattr() });
        result res;
        tx::wrap_collection_call(res, [&](result& r) {
            r = item.doc().collection_ref().mutate_in(item.doc().id(), specs, mutate_in_options().cas(item.doc().cas()));
        });
        ctx.trace("rollback result {}", res);
        ctx.hooks_.after_rollback_replace_or_remove(&ctx, item.doc().id());

    } catch (const client_error& e) {
        auto ec = e.ec();
        if (ctx.expiry_overtime_mode_) {
            throw transaction_operation_failed(FAIL_EXPIRY, std::string("expired while handling ") + e.what()).no_rollback();
        }
        switch (ec) {
            case FAIL_HARD:
            case FAIL_DOC_NOT_FOUND:
            case FAIL_CAS_MISMATCH:
                throw transaction_operation_failed(ec, e.what()).no_rollback();
            case FAIL_EXPIRY:
                ctx.expiry_overtime_mode_ = true;
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
            ctx.check_expiry_during_commit_or_rollback(STAGE_COMMIT_DOC, boost::optional<const std::string>(item.doc().id()));
            ctx.hooks_.before_doc_committed(&ctx, item.doc().id());

            // move staged content into doc
            ctx.trace("commit doc id {}, content {}, cas {}", item.doc().id(), item.content<nlohmann::json>().dump(), item.doc().cas());
            result res;
            if (item.type() == staged_mutation_type::INSERT && !cas_zero_mode) {
                tx::wrap_collection_call(
                  res, [&](result& r) { r = item.doc().collection_ref().insert(item.doc().id(), item.doc().content<nlohmann::json>()); });
            } else {
                tx::wrap_collection_call(res, [&](result& r) {
                    r = item.doc().collection_ref().mutate_in(item.doc().id(),
                                                              {
                                                                mutate_in_spec::upsert(TRANSACTION_INTERFACE_PREFIX_ONLY, nullptr).xattr(),
                                                                mutate_in_spec::remove(TRANSACTION_INTERFACE_PREFIX_ONLY).xattr(),
                                                                mutate_in_spec::fulldoc_upsert(item.content<nlohmann::json>()),
                                                              },
                                                              mutate_in_options()
                                                                .cas(cas_zero_mode ? 0 : item.doc().cas())
                                                                .store_semantics(subdoc_store_semantics::replace)
                                                                .durability(attempt_context_impl::durability(ctx.config_)));
                });
            }
            ctx.trace("commit doc result {}", res);
            // TODO: mutation tokens
            ctx.hooks_.after_doc_committed_before_saving_cas(&ctx, item.doc().id());
            item.doc().cas(res.cas);
            ctx.hooks_.after_doc_committed(&ctx, item.doc().id());
        } catch (const client_error& e) {
            error_class ec = e.ec();
            if (ctx.expiry_overtime_mode_) {
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
            ctx.check_expiry_during_commit_or_rollback(STAGE_REMOVE_DOC, boost::optional<const std::string>(item.doc().id()));
            ctx.hooks_.before_doc_removed(&ctx, item.doc().id());
            result res;
            tx::wrap_collection_call(res, [&](result& r) { r = item.doc().collection_ref().remove(item.doc().id()); });
            ctx.hooks_.after_doc_removed_pre_retry(&ctx, item.doc().id());
        } catch (const client_error& e) {
            error_class ec = e.ec();
            if (ctx.expiry_overtime_mode_) {
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
