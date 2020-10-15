#include <couchbase/client/result.hxx>
#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/staged_mutation.hxx>
#include <couchbase/transactions/transaction_fields.hxx>
#include <spdlog/spdlog.h>
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
tx::staged_mutation_queue::commit(attempt_context& ctx)
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
tx::staged_mutation_queue::commit_doc(attempt_context& ctx, staged_mutation& item, bool ambiguity_resolution_mode)
{
    try {
        ctx.check_expiry_during_commit_or_rollback(STAGE_COMMIT_DOC, boost::optional<const std::string>(item.doc().id()));
        ctx.hooks_.before_doc_committed(&ctx, item.doc().id());

        // move staged content into doc
        result res;
        if (item.type() == staged_mutation_type::INSERT) {
            ctx.wrap_collection_call(
              res, [&](result& r) { r = item.doc().collection_ref().insert(item.doc().id(), item.doc().content<nlohmann::json>()); });
        } else {
            ctx.wrap_collection_call(res, [&](result& r) {
                r = item.doc().collection_ref().mutate_in(item.doc().id(),
                                                          {
                                                            mutate_in_spec::upsert(TRANSACTION_INTERFACE_PREFIX_ONLY, nullptr).xattr(),
                                                            mutate_in_spec::fulldoc_upsert(item.content<nlohmann::json>()),
                                                          },
                                                          mutate_in_options().cas(item.doc().cas()));
            });
        }
        // TODO: mutation tokens
        ctx.hooks_.after_doc_committed_before_saving_cas(&ctx, item.doc().id());
        item.doc().cas(res.cas);
        ctx.hooks_.after_doc_committed(&ctx, item.doc().id());
    } catch (const client_error& e) {
        error_class ec = e.ec();
        if (ctx.expiry_overtime_mode_) {
            // TODO new final exception type expired_post_commit
            throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
        }
        switch (ec) {
            case FAIL_AMBIGUOUS:
                ctx.overall_.retry_delay(ctx.config_);
                return commit_doc(ctx, item, true);
            case FAIL_CAS_MISMATCH:
            case FAIL_DOC_ALREADY_EXISTS:
                if (ambiguity_resolution_mode) {
                    throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
                }
                ctx.overall_.retry_delay(ctx.config_);
                return commit_doc(ctx, item, true);
            default:
                throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
        }
    }
}

void
tx::staged_mutation_queue::remove_doc(attempt_context& ctx, staged_mutation& item)
{
    try {
        ctx.check_expiry_during_commit_or_rollback(STAGE_REMOVE_DOC, boost::optional<const std::string>(item.doc().id()));
        ctx.hooks_.before_doc_removed(&ctx, item.doc().id());
        result res;
        ctx.wrap_collection_call(
          res, [&](result& r) { r = item.doc().collection_ref().remove(item.doc().id(), remove_options().cas(item.doc().cas())); });
        // TODO:mutation tokens
    } catch (const client_error& e) {
        error_class ec = e.ec();
        if (ctx.expiry_overtime_mode_) {
            // TODO new final exception type expired_post_commit
            throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
        }
        switch (ec) {
            case FAIL_AMBIGUOUS:
                ctx.overall_.retry_delay(ctx.config_);
                return remove_doc(ctx, item);
            default:
                throw transaction_operation_failed(ec, e.what()).no_rollback().failed_post_commit();
        }
    }
}
