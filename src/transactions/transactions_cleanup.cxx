#include <chrono>
#include <functional>
#include <iostream>

#include <unistd.h>

#include <couchbase/client/cluster.hxx>
#include <couchbase/client/collection.hxx>
#include <couchbase/transactions/client_record.hxx>
#include <couchbase/transactions/transaction_fields.hxx>
#include <couchbase/transactions/transactions_cleanup.hxx>

#include "atr_ids.hxx"
#include "logging.hxx"
#include "uid_generator.hxx"

namespace tx = couchbase::transactions;

tx::transactions_cleanup_attempt::transactions_cleanup_attempt(const tx::atr_cleanup_entry& entry)
  : success_(false)
  , atr_id_(entry.atr_id_)
  , attempt_id_(entry.attempt_id_)
  , atr_bucket_name_(entry.atr_collection_->bucket_name())
{
}

tx::transactions_cleanup::transactions_cleanup(couchbase::cluster& cluster, const tx::transaction_config& config)
  : cluster_(cluster)
  , config_(config)
  , client_uuid_(uid_generator::next())
{
    // TODO: re-enable after fixing the loop
    // lost_attempts_thr = std::thread(std::bind(&transactions_cleanup::lost_attempts_loop, this));

    if (config_.cleanup_client_attempts()) {
        running_ = true;
        cleanup_thr_ = std::thread(std::bind(&transactions_cleanup::attempts_loop, this));
    } else {
        running_ = false;
    }
}

static uint64_t
byteswap64(uint64_t val)
{
    size_t ii;
    uint64_t ret = 0;
    for (ii = 0; ii < sizeof(uint64_t); ii++) {
        ret <<= 8ull;
        ret |= val & 0xffull;
        val >>= 8ull;
    }
    return ret;
}

/**
 * ${Mutation.CAS} is written by kvengine with 'macroToString(htonll(info.cas))'.  Discussed this with KV team and, though there is
 * consensus that this is off (htonll is definitely wrong, and a string is an odd choice), there are clients (SyncGateway) that consume the
 * current string, so it can't be changed.  Note that only little-endian servers are supported for Couchbase, so the 8 byte long inside the
 * string will always be little-endian ordered.
 *
 * Looks like: "0x000058a71dd25c15"
 * Want:        0x155CD21DA7580000   (1539336197457313792 in base10, an epoch time in millionths of a second)
 */
static uint64_t
parse_mutation_cas(const std::string& cas)
{
    if (cas.empty()) {
        return 0;
    }
    return byteswap64(stoull(cas, nullptr, 16)) / 1000000;
}

#define CLIENT_RECORD_DOC_ID "txn-client-record"
#define FIELD_CLIENTS "clients"
#define FIELD_HEARTBEAT "heartbeat_ms"
#define FIELD_EXPIRES "expires_ms"
#define SAFETY_MARGIN_EXPIRY_MS 2000

void
tx::transactions_cleanup::lost_attempts_loop()
{
    auto names = cluster_.buckets();
    std::list<std::thread> workers;
    for (const auto& name : names) {
        auto bkt = cluster_.bucket(name);
        auto uid = client_uuid_;
        auto config = config_;
        auto col = bkt->default_collection();
        workers.emplace_back([&]() {
            while (running_) {
                col->mutate_in(CLIENT_RECORD_DOC_ID,
                               {
                                 mutate_in_spec::upsert("dummy", nullptr).xattr(),
                               });
                auto res = col->lookup_in(CLIENT_RECORD_DOC_ID, { lookup_in_spec::get(FIELD_CLIENTS).xattr() });
                std::vector<std::string> expired_client_uids;
                std::vector<std::string> active_client_uids;
                for (auto& client : res.values[0]->items()) {
                    const auto& other_client_uid = client.key();
                    auto cl = client.value();
                    uint64_t cas_ms = res.cas / 1000000;
                    uint64_t heartbeat_ms = parse_mutation_cas(cl[FIELD_HEARTBEAT].get<std::string>());
                    auto expires_ms = cl[FIELD_EXPIRES].get<uint64_t>();
                    uint64_t expired_period = cas_ms - heartbeat_ms;
                    bool has_expired = expired_period >= expires_ms;
                    if (has_expired && other_client_uid != uid) {
                        expired_client_uids.push_back(other_client_uid);
                    } else {
                        active_client_uids.push_back(other_client_uid);
                    }
                }
                if (std::find(active_client_uids.begin(), active_client_uids.end(), uid) == active_client_uids.end()) {
                    active_client_uids.push_back(uid);
                }
                std::vector<mutate_in_spec> specs;
                specs.push_back(mutate_in_spec::upsert(std::string(FIELD_CLIENTS) + "." + uid + "." + FIELD_HEARTBEAT, "${Mutation.CAS}")
                                  .xattr()
                                  .create_path()
                                  .expand_macro());
                specs.push_back(mutate_in_spec::upsert(std::string(FIELD_CLIENTS) + "." + uid + "." + FIELD_EXPIRES,
                                                       config.cleanup_window().count() / 2 + SAFETY_MARGIN_EXPIRY_MS)
                                  .xattr()
                                  .create_path());
                for (auto idx = 0; idx < std::min(expired_client_uids.size(), (size_t)14); idx++) {
                    specs.push_back(mutate_in_spec::remove(std::string(FIELD_CLIENTS) + "." + expired_client_uids[idx]).xattr());
                }
                col->mutate_in(CLIENT_RECORD_DOC_ID, specs);

                std::sort(active_client_uids.begin(), active_client_uids.end());
                size_t this_idx =
                  std::distance(active_client_uids.begin(), std::find(active_client_uids.begin(), active_client_uids.end(), uid));
                std::list<std::string> atrs_to_handle;
                auto all_atrs = atr_ids::all();
                size_t num_active_clients = active_client_uids.size();
                for (auto it = all_atrs.begin() + this_idx; it < all_atrs.end() + num_active_clients; it += num_active_clients) {
                    // clean the ATR entry
                    std::string atr_id = *it;
                    col->mutate_in(atr_id, { mutate_in_spec::upsert("dummy", nullptr).xattr() });
                    result atr = col->lookup_in(atr_id, { lookup_in_spec::get(ATR_FIELD_ATTEMPTS).xattr() });
                    uint64_t cas_ms = atr.cas / 1000000;
                    for (auto& kv : atr.values[0]->items()) {
                        const auto& attempt_id = kv.key();
                        auto entry = kv.value();
                        std::string status = entry[ATR_FIELD_STATUS].get<std::string>();
                        uint64_t start_ms = parse_mutation_cas(entry[ATR_FIELD_START_TIMESTAMP].get<std::string>());
                        uint64_t commit_ms = parse_mutation_cas(entry[ATR_FIELD_START_COMMIT].get<std::string>());
                        uint64_t complete_ms = parse_mutation_cas(entry[ATR_FIELD_TIMESTAMP_COMPLETE].get<std::string>());
                        uint64_t rollback_ms = parse_mutation_cas(entry[ATR_FIELD_TIMESTAMP_ROLLBACK_START].get<std::string>());
                        uint64_t rolledback_ms = parse_mutation_cas(entry[ATR_FIELD_TIMESTAMP_ROLLBACK_COMPLETE].get<std::string>());
                        int expires_after_ms = entry[ATR_FIELD_EXPIRES_AFTER_MSECS].get<int>();
                        auto inserted_ids = entry[ATR_FIELD_DOCS_INSERTED];
                        auto replaced_ids = entry[ATR_FIELD_DOCS_REPLACED];
                        auto removed_ids = entry[ATR_FIELD_DOCS_REMOVED];

                        const uint64_t safety_margin_ms = 2500;
                        bool has_expired = false;
                        if (start_ms > 0) {
                            has_expired = (cas_ms - start_ms) > (expires_after_ms + safety_margin_ms);
                        }
                        if (!has_expired) {
                            continue;
                        }
                        if (status == "COMMITTED") {
                            for (auto& id : inserted_ids) {
                                result doc = col->lookup_in(id.get<std::string>(),
                                                            {
                                                              lookup_in_spec::get(ATR_ID).xattr(),
                                                              lookup_in_spec::get(TRANSACTION_ID).xattr(),
                                                              lookup_in_spec::get(ATTEMPT_ID).xattr(),
                                                              lookup_in_spec::get(STAGED_DATA).xattr(),
                                                              lookup_in_spec::get(ATR_BUCKET_NAME).xattr(),
                                                              lookup_in_spec::get(ATR_COLL_NAME).xattr(),
                                                              lookup_in_spec::get(TRANSACTION_RESTORE_PREFIX_ONLY).xattr(),
                                                              lookup_in_spec::get(TYPE).xattr(),
                                                              lookup_in_spec::get("$document").xattr(),
                                                              lookup_in_spec::fulldoc_get(),
                                                            });
                            }
                        } else if (status == "ABORTED") {
                            // TODO:
                        } else if (status == "PENDING") {
                            // TODO:
                        } else {
                            // TODO:
                        }
                    }
                }

                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        });
    }
    for (auto& thr : workers) {
        thr.join();
    }
}
void
tx::transactions_cleanup::force_cleanup_entry(atr_cleanup_entry& entry, transactions_cleanup_attempt& attempt)
{
    try {
        entry.clean(*this, &attempt);
        attempt.success(true);

    } catch (const std::runtime_error& e) {
        spdlog::error("error attempting to clean {}: {}", entry, e.what());
        attempt.success(false);
    }
}

void
tx::transactions_cleanup::force_cleanup_attempts(std::vector<transactions_cleanup_attempt>& results)
{
    spdlog::trace("starting force_cleanup_attempts");
    while (atr_queue_.size() > 0) {
        auto entry = atr_queue_.pop(false);
        if (!entry) {
            spdlog::error("pop failed to return entry, but queue size {}", atr_queue_.size());
            return;
        }
        results.emplace_back(*entry);
        try {
            entry->clean(*this, &(results.back()));
            results.back().success(true);
        } catch (std::runtime_error& e) {
            results.back().success(false);
        }
    }
}

void
tx::transactions_cleanup::attempts_loop()
{
    spdlog::info("cleanup attempts loop starting...");
    while (running_) {
        auto entry = atr_queue_.pop();
        if (entry) {
            spdlog::trace("beginning cleanup on {}", *entry);
            try {
                entry->clean(*this);
            } catch (const std::runtime_error& e) {
                // TODO: perhaps in config later?
                auto backoff_duration = std::chrono::milliseconds(10000);
                entry->min_start_time(std::chrono::system_clock::now() + backoff_duration);
                spdlog::info("got error '{}' cleaning {}, will retry in {} seconds",
                             e.what(),
                             entry,
                             std::chrono::duration_cast<std::chrono::seconds>(backoff_duration).count());
                atr_queue_.push(*entry);
            } catch (...) {
                // TODO: perhaps in config later?
                auto backoff_duration = std::chrono::milliseconds(10000);
                entry->min_start_time(std::chrono::system_clock::now() + backoff_duration);
                spdlog::info("got error cleaning {}, will retry in {} seconds",
                             entry,
                             std::chrono::duration_cast<std::chrono::seconds>(backoff_duration).count());
                atr_queue_.push(*entry);
            }
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    spdlog::info("attempts_loop stopping - {} entries on queue", atr_queue_.size());
}

void
tx::transactions_cleanup::add_attempt(attempt_context& ctx)
{
    if (ctx.attempt_state() == tx::attempt_state::NOT_STARTED) {
        spdlog::trace("attempt not started, not adding to cleanup");
        return;
    }
    if (config_.cleanup_client_attempts()) {
        spdlog::trace("adding attempt {} to cleanup queue", ctx.attempt_id());
        atr_queue_.push(ctx);
    } else {
        spdlog::trace("not cleaning client attempts, ignoring {}", ctx.attempt_id());
    }
}
void
tx::transactions_cleanup::close()
{
    running_ = false;
    // TODO: re-enable after fixing the loop
    // lost_attempts_thr.join();
    if (cleanup_thr_.joinable()) {
        cleanup_thr_.join();
        spdlog::info("cleanup closed");
    }
}
tx::transactions_cleanup::~transactions_cleanup()
{
    close();
}
