#include <couchbase/client/collection.hxx>

#include <spdlog/fmt/ostr.h>

#include <couchbase/transactions.hxx>
#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/attempt_state.hxx>

#include "atr_ids.hxx"
#include "logging.hxx"

namespace couchbase
{
namespace transactions
{

    attempt_context::attempt_context(transactions* parent, transaction_context& transaction_ctx, const transaction_config& config)
      : parent_(parent)
      , overall_(transaction_ctx)
      , config_(config)
      , atr_collection_(nullptr)
      , is_done_(false)
      , hooks_(config.attempt_context_hooks())
    {
        // put a new transaction_attempt in the context...
        overall_.add_attempt();
        spdlog::trace("added new attempt id {} state {}", id(), state());
    }

    transaction_document attempt_context::get(std::shared_ptr<couchbase::collection> collection, const std::string& id)
    {
        auto result = get_optional(collection, id);
        if (result) {
            spdlog::trace("get returning {}", *result);
            return result.get();
        }
        spdlog::error("Document with id {} not found", id);
        throw transaction_operation_failed(FAIL_DOC_NOT_FOUND, "Document not found");
    }

    boost::optional<transaction_document> attempt_context::get_optional(std::shared_ptr<couchbase::collection> collection,
                                                                        const std::string& id)
    {
        auto retval = do_get(collection, id);
        hooks_.after_get_complete(this, id);
        return retval;
    }

    transaction_document attempt_context::replace_raw(std::shared_ptr<couchbase::collection> collection,
                                                      const transaction_document& document,
                                                      const nlohmann::json& content)
    {
        try {
            spdlog::trace("replacing {} with {}", document, content);
            check_if_done();
            select_atr_if_needed(collection, document.id());
            check_and_handle_blocking_transactions(document);
            check_expiry_pre_commit(STAGE_REPLACE, document.id());
            set_atr_pending_if_first_mutation(collection);

            std::vector<mutate_in_spec> specs = {
                mutate_in_spec::upsert(TRANSACTION_ID, overall_.transaction_id()).xattr().create_path(),
                mutate_in_spec::upsert(ATTEMPT_ID, id()).xattr().create_path(),
                mutate_in_spec::insert(STAGED_DATA, content).xattr().create_path(),
                mutate_in_spec::upsert(ATR_ID, atr_id_.value()).xattr().create_path(),
                mutate_in_spec::upsert(ATR_BUCKET_NAME, collection->bucket_name()).xattr(),
                mutate_in_spec::upsert(ATR_COLL_NAME, collection->scope() + "." + collection->name()).xattr(),
                mutate_in_spec::upsert(CRC32_OF_STAGING, mutate_in_macro::VALUE_CRC_32C).xattr().create_path().expand_macro(),
                mutate_in_spec::upsert(TYPE, "replace").xattr(),
            };
            if (document.metadata()) {
                if (document.metadata()->cas()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_CAS, document.metadata()->cas().value()).create_path().xattr());
                }
                if (document.metadata()->revid()) {
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_REVID, document.metadata()->revid().value()).create_path().xattr());
                }
                if (document.metadata()->exptime()) {
                    specs.emplace_back(
                      mutate_in_spec::upsert(PRE_TXN_EXPTIME, document.metadata()->exptime().value()).create_path().xattr());
                }
            }

            couchbase::result res;
            hooks_.before_staged_replace(this, document.id());
            spdlog::trace("about to replace doc {} with cas {} in txn {}", document.id(), document.cas(), overall_.transaction_id());
            wrap_collection_call(res, [&](couchbase::result& r) {
                r = collection->mutate_in(
                  document.id(),
                  specs,
                  mutate_in_options().cas(document.cas()).access_deleted(document.links().is_deleted()).durability(durability(config_)));
            });
            hooks_.after_staged_replace_complete(this, document.id());
            transaction_document out = document;
            out.cas(res.cas);
            // now handle replace-replace, or insert-replace
            staged_mutation* mutation = staged_mutations_.find_replace(collection, document.id());
            if (mutation != nullptr) {
                spdlog::trace("document {} was replaced already in txn, replacing again", document.id());
                // only thing that we need to change are the content, cas
                mutation->content(content);
                mutation->doc().cas(out.cas());
            }
            mutation = staged_mutations_.find_insert(collection, document.id());
            if (mutation != nullptr) {
                spdlog::trace("document {} replaced after insert in this txn", document.id());
                // only thing that we need to change are the content, cas
                mutation->doc().content(content);
                mutation->doc().cas(out.cas());
            } else {
                staged_mutations_.add(staged_mutation(out, content, staged_mutation_type::REPLACE));
                add_mutation_token();
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
    }

    transaction_document attempt_context::insert_raw(std::shared_ptr<couchbase::collection> collection,
                                                     const std::string& id,
                                                     const nlohmann::json& content)
    {
        try {
            check_if_done();
            select_atr_if_needed(collection, id);
            if (check_for_own_write(collection, id)) {
                throw transaction_operation_failed(FAIL_OTHER,
                                                   "cannot insert a document that has already been mutated in this transaction");
            }
            check_expiry_pre_commit(STAGE_INSERT, id);
            set_atr_pending_if_first_mutation(collection);
            uint64_t cas = 0;
            return retry_op<transaction_document>(
              [&]() -> transaction_document { return create_staged_insert(collection, id, content, cas); });
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
                    throw retry_operation("FAIL AMBIGUOUS in insert");
                case FAIL_OTHER:
                    throw transaction_operation_failed(ec, e.what());
                case FAIL_HARD:
                    throw transaction_operation_failed(ec, e.what()).no_rollback();
                default:
                    throw transaction_operation_failed(FAIL_OTHER, e.what()).retry();
            }
        }
    }

    void attempt_context::select_atr_if_needed(std::shared_ptr<couchbase::collection> collection, const std::string& id)
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
            atr_collection_ = collection;
            overall_.atr_collection(collection->name());
            overall_.atr_id(*atr_id_);
            state(attempt_state::NOT_STARTED);
            spdlog::info("first mutated doc in transaction is \"{}\" on vbucket {}, so using atr \"{}\"", id, vbucket_id, atr_id_.value());
        }
    }

    void attempt_context::check_atr_entry_for_blocking_document(const transaction_document& doc)
    {
        auto collection = parent_->cluster_ref().bucket(doc.links().atr_bucket_name().value())->default_collection();
        int retries = 0;
        while (retries < 5) {
            retries++;
            try {
                hooks_.before_check_atr_entry_for_blocking_doc(this, doc.id());
                auto atr = active_transaction_record::get_atr(collection, doc.links().atr_id().value());
                if (atr) {
                    auto entries = atr->entries();
                    auto it = std::find_if(entries.begin(), entries.end(), [&doc](const atr_entry& e) {
                        return e.attempt_id() == doc.links().staged_attempt_id();
                    });
                    if (it != entries.end()) {
                        if (it->has_expired()) {
                            spdlog::trace("existing atr entry has expired, ignoring");
                            return;
                        }
                        switch (it->state()) {
                            case attempt_state::COMPLETED:
                            case attempt_state::ROLLED_BACK:
                                spdlog::trace("existing atr entry can be ignored due to state {}", it->state());
                                return;
                            default:
                                spdlog::trace("existing atr entry found in state {}, retrying in 100ms", it->state());
                        }
                        // TODO  this (and other retries) probably need a clever class, exponential backoff, etc...
                        std::this_thread::sleep_for(std::chrono::milliseconds(50 * retries));
                    } else {
                        spdlog::trace("no blocking atr entry");
                        return;
                    }
                }
                // if we are here, there is still a write-write conflict
                throw transaction_operation_failed(FAIL_WRITE_WRITE_CONFLICT, "document is in another transaction").retry();
            } catch (const client_error& e) {
                throw transaction_operation_failed(FAIL_WRITE_WRITE_CONFLICT, e.what()).retry();
            }
        }
    }

    void attempt_context::remove(std::shared_ptr<collection> collection, transaction_document& document)
    {
        try {
            check_if_done();
            check_expiry_pre_commit(STAGE_REMOVE, document.id());
            // TODO - look for staged insert
            if (staged_mutations_.find_insert(collection, document.id())) {
                spdlog::error("cannot remove document {}, as it was inserted in this transaction", document.id());
                throw transaction_operation_failed(FAIL_OTHER, "Cannot remove a document inserted in the same transaction");
            }
            check_and_handle_blocking_transactions(document);
            select_atr_if_needed(collection, document.id());

            set_atr_pending_if_first_mutation(collection);

            hooks_.before_staged_remove(this, document.id());
            spdlog::info("about to remove remove doc {} with cas {}", document.id(), document.cas());
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
                    specs.emplace_back(mutate_in_spec::upsert(PRE_TXN_REVID, document.metadata()->revid().value()).create_path().xattr());
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
                  mutate_in_options().durability(durability(config_)).access_deleted(document.links().is_deleted()).cas(document.cas()));
            });
            spdlog::info("removed doc {} CAS={}, rc={}", document.id(), res.cas, res.strerror());
            hooks_.after_staged_remove_complete(this, document.id());
            document.cas(res.cas);
            staged_mutations_.add(staged_mutation(document, "", staged_mutation_type::REMOVE));
            add_mutation_token();
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
    }

    void attempt_context::commit()
    {
        spdlog::info("commit {}", id());
        check_expiry_pre_commit(STAGE_BEFORE_COMMIT, {});
        if (atr_collection_ && atr_id_ && !is_done_) {
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + id() + ".");
            std::vector<mutate_in_spec> specs({
              mutate_in_spec::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::COMMITTED)).xattr(),
              mutate_in_spec::upsert(prefix + ATR_FIELD_START_COMMIT, "${Mutation.CAS}").xattr().expand_macro(),
            });
            try {
                hooks_.before_atr_commit(this);
                staged_mutations_.extract_to(prefix, specs);
                result res;
                wrap_collection_call(res, [&](result& r) {
                    r = atr_collection_->mutate_in(atr_id_.value(), specs);
                    hooks_.after_atr_commit(this);
                });
                state(attempt_state::COMMITTED);
                std::vector<transaction_document> docs;
                staged_mutations_.commit(*this);
                try {
                    // if this succeeds, set ATR to COMPLETED
                    std::string prefix(ATR_FIELD_ATTEMPTS + "." + id() + ".");
                    std::vector<mutate_in_spec> specs({
                      mutate_in_spec::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::COMPLETED)).xattr(),
                      mutate_in_spec::upsert(prefix + ATR_FIELD_TIMESTAMP_COMPLETE, "${Mutation.CAS}").xattr().expand_macro(),
                    });
                    result atr_res;
                    hooks_.before_atr_complete(this);
                    // also post commit
                    check_expiry_pre_commit(STAGE_ATR_COMPLETE, {});
                    wrap_collection_call(atr_res, [&](result& r) { r = atr_collection_->mutate_in(atr_id_.value(), specs); });
                    spdlog::trace("setting attempt state COMPLETED for attempt {}", atr_id_.value());
                    hooks_.after_atr_complete(this);
                    state(attempt_state::COMPLETED);
                } catch (const client_error& er) {
                    error_class ec = er.ec();
                    switch (ec) {
                        case FAIL_HARD:
                            throw transaction_operation_failed(ec, er.what()).no_rollback();
                        default:
                            spdlog::info("ignoring error marking ATR completed: {}", er.what());
                    }
                }
                is_done_ = true;
            } catch (const client_error& e) {
                error_class ec = e.ec();
                switch (ec) {
                    case FAIL_HARD:
                        throw transaction_operation_failed(ec, e.what()).no_rollback();
                    default:
                        spdlog::error("failed to commit transaction {}, attempt {}, with error {}", transaction_id(), id(), e.what());
                }
            }
        } else {
            // no mutation, no need to commit
            if (!is_done_) {
                spdlog::info("calling commit on attempt that has got no mutations, skipping");
                is_done_ = true;
                return;
            } else {
                // do not rollback or retry
                throw transaction_operation_failed(FAIL_OTHER, "calling commit on attempt that is already completed").no_rollback();
            }
        }
    }

    void attempt_context::rollback()
    {
        spdlog::info("rolling back");
        // check for expiry
        check_expiry_during_commit_or_rollback(STAGE_ROLLBACK, boost::none);
        if (!atr_id_ || !atr_collection_ || state() == attempt_state::NOT_STARTED) {
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
            throw transaction_operation_failed(FAIL_OTHER, msg).no_rollback();
        }
        // We do 3 things - set the atr to abort
        //                - unstage the docs
        //                - set atr to ROLLED_BACK
        std::string prefix(ATR_FIELD_ATTEMPTS + "." + id() + ".");
        std::vector<mutate_in_spec> specs({
          mutate_in_spec::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::ABORTED)).xattr(),
          mutate_in_spec::upsert(prefix + ATR_FIELD_TIMESTAMP_ROLLBACK_START, "${Mutation.CAS}").xattr().expand_macro(),
        });
        // now add the staged mutations...
        staged_mutations_.extract_to(prefix, specs);

        try {
            hooks_.before_atr_aborted(this);
            result res;
            wrap_collection_call(res, [&](result& r) { r = atr_collection_->mutate_in(atr_id_.value(), specs); });
            state(attempt_state::ABORTED);
            hooks_.after_atr_aborted(this);
            spdlog::trace("rollback completed atr abort phase");
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
                        spdlog::trace("rollback result {}", r);
                        hooks_.after_rollback_replace_or_remove(this, mutation.doc().id());
                        // TODO: deal with errors mutating the doc
                        break;
                }
                spdlog::trace("rollback completed unstaging docs");
            });

            // now complete the atr rollback
            hooks_.before_atr_rolled_back(this);
            std::vector<mutate_in_spec> specs({
              mutate_in_spec::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::ROLLED_BACK)).xattr(),
              mutate_in_spec::upsert(prefix + ATR_FIELD_TIMESTAMP_ROLLBACK_COMPLETE, "${Mutation.CAS}").xattr().expand_macro(),
            });
            result atr_res;
            wrap_collection_call(atr_res, [&](result& r) { r = atr_collection_->mutate_in(atr_id_.value(), specs); });
            state(attempt_state::ROLLED_BACK);
            hooks_.after_atr_rolled_back(this);
            is_done_ = true;
            // TODO: deal with errors mutating ATR record, and retries perhaps?
        } catch (const client_error& e) {
            error_class ec = e.ec();
            spdlog::error("rollback transaction {}, attempt {} fail with error {}", transaction_id(), id(), e.what());
            if (ec == FAIL_HARD) {
                throw transaction_operation_failed(ec, e.what()).no_rollback();
            }
        }
    }

    bool attempt_context::has_expired_client_side(std::string place, boost::optional<const std::string> doc_id)
    {
        bool over = overall_.has_expired_client_side(config_);
        bool hook = hooks_.has_expired_client_side(this, place, doc_id);
        if (over) {
            spdlog::info("{} expired in {}", id(), place);
        }
        if (hook) {
            spdlog::info("{} fake expiry in {}", id(), place);
        }
        return over || hook;
    }

    void attempt_context::check_expiry_pre_commit(std::string stage, boost::optional<const std::string> doc_id)
    {
        if (has_expired_client_side(stage, std::move(doc_id))) {
            spdlog::info("{} has expired in stage {}, entering expiry-overtime mode - will make one attempt to rollback", id(), stage);

            // [EXP-ROLLBACK] Combo of setting this mode and throwing AttemptExpired will result in a attempt to rollback, which will
            // ignore expiries, and bail out if anything fails
            expiry_overtime_mode_ = true;
            throw client_error(FAIL_EXPIRY, std::string("Attempt has expired in stage ") + stage);
        }
    }

    void attempt_context::check_expiry_during_commit_or_rollback(const std::string& stage, boost::optional<const std::string> doc_id)
    {
        // [EXP-COMMIT-OVERTIME]
        if (!expiry_overtime_mode_) {
            if (has_expired_client_side(stage, std::move(doc_id))) {
                spdlog::info("{} has expired in stage {}, entering expiry-overtime mode (one attempt to complete commit)", id(), stage);
                expiry_overtime_mode_ = true;
            }
        } else {
            spdlog::info("{} ignoring expiry in stage {}  as in expiry-overtime mode", id(), stage);
        }
    }

    void attempt_context::insure_atr_exists(std::shared_ptr<collection> collection)
    {
        // TODO: use exists here when collection supports it
        result res = collection->get(atr_id_.value());
        // just upsert an empty doc for now
        if (res.is_success()) {
            return;
        }
        collection->upsert(atr_id_.value(), nlohmann::json::object());
    }
    void attempt_context::set_atr_pending_if_first_mutation(std::shared_ptr<collection> collection)
    {
        if (staged_mutations_.empty()) {
            std::string prefix(ATR_FIELD_ATTEMPTS + "." + id() + ".");
            if (!atr_id_) {
                throw transaction_operation_failed(FAIL_OTHER, std::string("ATR ID is not initialized"));
            }
            insure_atr_exists(collection);
            result res;
            try {
                check_expiry_during_commit_or_rollback(STAGE_ATR_PENDING, {});
                hooks_.before_atr_pending(this);
                spdlog::info("updating atr {}", atr_id_.value());
                wrap_collection_call(res, [&](result& r) {
                    r = collection->mutate_in(

                      atr_id_.value(),
                      { mutate_in_spec::insert(prefix + ATR_FIELD_TRANSACTION_ID, overall_.transaction_id()).xattr().create_path(),
                        mutate_in_spec::insert(prefix + ATR_FIELD_STATUS, attempt_state_name(attempt_state::PENDING)).xattr().create_path(),
                        mutate_in_spec::insert(prefix + ATR_FIELD_START_TIMESTAMP, mutate_in_macro::CAS).xattr().expand_macro(),
                        mutate_in_spec::insert(prefix + ATR_FIELD_EXPIRES_AFTER_MSECS,
                                               std::chrono::duration_cast<std::chrono::milliseconds>(config_.expiration_time()).count())
                          .xattr() },
                      mutate_in_options().durability(durability(config_)));
                });
                state(attempt_state::PENDING);
                spdlog::info("set ATR {}/{}/{} to Pending, got CAS (start time) {}",
                             collection->bucket_name(),
                             collection->name(),
                             atr_id_.value(),
                             res.cas);
            } catch (const client_error& e) {
                spdlog::trace("caught {}, ec={}", e.what(), e.ec());
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
            start_time_server_ = std::chrono::nanoseconds(res.cas);
            hooks_.after_atr_pending(this);
        }
    }

    staged_mutation* attempt_context::check_for_own_write(std::shared_ptr<collection> collection, const std::string& id)
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

    void attempt_context::check_and_handle_blocking_transactions(const transaction_document& doc)
    {
        // The main reason to require doc to be fetched inside the transaction is so we can detect this on the client side
        if (doc.links().has_staged_write()) {
            // Check not just writing the same doc twice in the same transaction
            // NOTE: we check the transaction rather than attempt id. This is to handle [RETRY-ERR-AMBIG-REPLACE].
            if (doc.links().staged_transaction_id().value() == overall_.transaction_id()) {
                spdlog::info("doc {} has been written by this transaction, ok to continue", doc.id());
            } else {
                if (doc.links().atr_id() && doc.links().atr_bucket_name() && doc.links().staged_attempt_id()) {
                    spdlog::info("doc {} in another txn, checking atr...", doc.id());
                    check_atr_entry_for_blocking_document(doc);
                } else {
                    spdlog::info("doc {} is in another transaction {}, but doesn't have enough info to check the atr. "
                                 "probably a bug, proceeding to overwrite",
                                 doc.id(),
                                 doc.links().staged_attempt_id().get());
                }
            }
        }
    }

    void attempt_context::check_if_done()
    {
        if (is_done_) {
            throw transaction_operation_failed(FAIL_OTHER, "Cannot perform operations after transaction has been committed or rolled back")
              .no_rollback();
        }
    }

    void attempt_context::wrap_collection_call(result& res, std::function<void(result&)> call)
    {
        call(res);
        if (!res.is_success()) {
            throw client_error(res);
        }
    }

    boost::optional<transaction_document> attempt_context::do_get(std::shared_ptr<collection> collection, const std::string& id)
    {
        try {
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

            auto doc_res = get_doc(collection, id);
            if (!doc_res) {
                return {};
            }
            auto doc = doc_res->first;
            auto get_doc_res = doc_res->second;
            if (doc.links().is_document_in_transaction()) {
                spdlog::trace("doc {} in transaction", doc);
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
                    auto status = doc.status();
                    if (entry) {
                        if (doc.links().staged_attempt_id() && entry->attempt_id() == this->id()) {
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
            } else {
                if (get_doc_res.is_deleted) {
                    spdlog::trace("doc not in txn, and is_deleted, so not returning it.");
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
                    spdlog::warn(what);
                    throw transaction_operation_failed(FAIL_OTHER, what);
                }
            }
        }
    }

    boost::optional<std::pair<transaction_document, couchbase::result>> attempt_context::get_doc(
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
                                            lookup_in_spec::fulldoc_get() },
                                          lookup_in_options().access_deleted(true));
            });
            return std::pair<transaction_document, result>(
              transaction_document::create_from(*collection, id, res, transaction_document_status::NORMAL), res);
        } catch (const client_error& e) {
            if (e.ec() == FAIL_DOC_NOT_FOUND) {
                return boost::none;
            }
            throw;
        }
    }

    transaction_document attempt_context::create_staged_insert(std::shared_ptr<collection> collection,
                                                               const std::string& id,
                                                               const nlohmann::json& content,
                                                               uint64_t& cas)
    {
        try {
            hooks_.before_staged_insert(this, id);
            spdlog::info("about to insert staged doc {} with cas {}", id, cas);
            check_expiry_pre_commit(STAGE_CREATE_STAGED_INSERT, id);
            result res;
            wrap_collection_call(res, [&](result& r) {
                r = collection->mutate_in(
                  id,
                  {
                    mutate_in_spec::insert(TRANSACTION_ID, overall_.transaction_id()).xattr().create_path(),
                    mutate_in_spec::insert(ATTEMPT_ID, this->id()).create_path().xattr(),
                    mutate_in_spec::insert(ATR_ID, atr_id_.value()).create_path().xattr(),
                    mutate_in_spec::insert(STAGED_DATA, content).create_path().xattr(),
                    mutate_in_spec::insert(ATR_BUCKET_NAME, collection->bucket_name()).create_path().xattr(),
                    mutate_in_spec::insert(ATR_COLL_NAME, collection->scope() + "." + collection->name()).xattr().create_path(),
                    mutate_in_spec::insert(TYPE, "insert").create_path().xattr(),
                    mutate_in_spec::insert(CRC32_OF_STAGING, mutate_in_macro::VALUE_CRC_32C).create_path().xattr().expand_macro(),
                  },
                  mutate_in_options().durability(durability(config_)).access_deleted(true).create_as_deleted(true).cas(cas));
            });
            spdlog::info("inserted doc {} CAS={}, rc={}", id, res.cas, res.strerror());
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
                                    true);
            transaction_document out(id, content, res.cas, *collection, links, transaction_document_status::NORMAL, boost::none);
            staged_mutations_.add(staged_mutation(out, content, staged_mutation_type::INSERT));
            add_mutation_token();
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
                    throw; // this gets handled in insert (and does a retry of entire insert)
                case FAIL_DOC_ALREADY_EXISTS:
                case FAIL_CAS_MISMATCH:
                    // special handling for doc already existing
                    try {
                        spdlog::trace("found existing doc {}, may still be able to insert", id);
                        hooks_.before_get_doc_in_exists_during_staged_insert(this, id);
                        auto get_document = get_doc(collection, id);
                        if (get_document) {
                            auto doc = get_document->first;
                            auto get_res = get_document->second;
                            spdlog::trace("document {} exists, is_in_transaction {}, is_deleted {} ",
                                          doc.id(),
                                          doc.links().is_document_in_transaction(),
                                          get_res.is_deleted);
                            if (!doc.links().is_document_in_transaction() && get_res.is_deleted) {
                                // it is just a deleted doc, so we are ok.  Lets try again, but with the cas
                                spdlog::trace("doc was deleted, retrying with cas {}", doc.cas());
                                cas = doc.cas();
                                throw retry_operation("create staged insert found existing deleted doc, retrying");
                            }
                            if (!doc.links().is_document_in_transaction()) {
                                // doc was inserted outside txn elsewhere
                                throw transaction_operation_failed(FAIL_DOC_ALREADY_EXISTS, "document already exists");
                            }
                            check_and_handle_blocking_transactions(doc);
                            // if the check didn't throw, we can retry staging with cas
                            spdlog::trace("doc ok to overwrite, retrying with cas {}", doc.cas());
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
                                spdlog::trace("transient error trying to get doc in insert - retrying txn");
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
