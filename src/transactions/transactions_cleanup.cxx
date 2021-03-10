#include <algorithm>
#include <chrono>
#include <functional>
#include <iostream>
#include <unistd.h>

#include <couchbase/client/cluster.hxx>
#include <couchbase/client/collection.hxx>

#include "active_transaction_record.hxx"
#include "atr_ids.hxx"
#include "attempt_context_impl.hxx"
#include "cleanup_testing_hooks.hxx"
#include "client_record.hxx"
#include "logging.hxx"
#include "transaction_fields.hxx"
#include "transactions_cleanup.hxx"
#include "uid_generator.hxx"
#include "utils.hxx"

namespace tx = couchbase::transactions;

tx::transactions_cleanup_attempt::transactions_cleanup_attempt(const tx::atr_cleanup_entry& entry)
  : success_(false)
  , atr_id_(entry.atr_id_)
  , attempt_id_(entry.attempt_id_)
  , atr_bucket_name_(entry.atr_collection_->bucket_name())
  , state_(attempt_state::NOT_STARTED)
{
}

tx::transactions_cleanup::transactions_cleanup(couchbase::cluster& cluster, const tx::transaction_config& config)
  : cluster_(cluster)
  , config_(config)
  , client_uuid_(uid_generator::next())
  , running_(false)
{
    if (config_.cleanup_lost_attempts()) {
        running_ = true;
        lost_attempts_thr_ = std::thread(std::bind(&transactions_cleanup::lost_attempts_loop, this));
    }

    if (config_.cleanup_client_attempts()) {
        running_ = true;
        cleanup_thr_ = std::thread(std::bind(&transactions_cleanup::attempts_loop, this));
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
 * ${Mutation.CAS} is written by kvengine with
 * 'macroToString(htonll(info.cas))'.  Discussed this with KV team and, though
 * there is consensus that this is off (htonll is definitely wrong, and a string
 * is an odd choice), there are clients (SyncGateway) that consume the current
 * string, so it can't be changed.  Note that only little-endian servers are
 * supported for Couchbase, so the 8 byte long inside the string will always be
 * little-endian ordered.
 *
 * Looks like: "0x000058a71dd25c15"
 * Want:        0x155CD21DA7580000   (1539336197457313792 in base10, an epoch
 * time in millionths of a second)
 */
static uint64_t
parse_mutation_cas(const std::string& cas)
{
    if (cas.empty()) {
        return 0;
    }
    return byteswap64(stoull(cas, nullptr, 16)) / 1000000;
}

static const std::string CLIENT_RECORD_DOC_ID = "_txn:client-record";
static const std::string FIELD_RECORDS = "records";
static const std::string FIELD_CLIENTS_ONLY = "clients";
static const std::string FIELD_CLIENTS = FIELD_RECORDS + "." + FIELD_CLIENTS_ONLY;
static const std::string FIELD_HEARTBEAT = "heartbeat_ms";
static const std::string FIELD_EXPIRES = "expires_ms";
static const std::string FIELD_OVERRIDE = "override";
static const std::string FIELD_OVERRIDE_EXPIRES = "expires";
static const std::string FIELD_OVERRIDE_ENABLED = "enabled";
#define SAFETY_MARGIN_EXPIRY_MS 2000

template<class R, class P>
bool
tx::transactions_cleanup::interruptable_wait(std::chrono::duration<R, P> delay)
{
    // wait for specified time, _or_ until the condition variable changes
    std::unique_lock<std::mutex> lock(mutex_);
    if (!running_.load()) {
        return false;
    }
    cv_.wait_for(lock, delay, [&] { return !running_.load(); });
    return running_.load();
}

void
tx::transactions_cleanup::clean_lost_attempts_in_bucket(const std::string& bucket_name)
{
    lost_attempts_cleanup_log->info("cleanup for {} starting", bucket_name);
    if (!running_.load()) {
        lost_attempts_cleanup_log->info("cleanup of {} complete", bucket_name);
        return;
    }
    auto coll = cluster_.bucket(bucket_name)->default_collection();
    auto details = get_active_clients(coll, client_uuid_);
    auto all_atrs = atr_ids::all();

    // we put a delay in between handling each atr, such that the delays add up to the total window time.  This
    // means this takes longer than the window.

    auto delay = std::chrono::milliseconds(config_.cleanup_window().count() / std::max(1ul, all_atrs.size() / details.num_active_clients));
    lost_attempts_cleanup_log->info("{} active clients (including this one), {} atrs to check {}ms delay between checking each atr",
                                    details.num_active_clients,
                                    all_atrs.size(),
                                    delay.count());
    for (auto it = all_atrs.begin() + details.index_of_this_client; it < all_atrs.end(); it += details.num_active_clients) {
        // clean the ATR entry
        std::string atr_id = *it;
        if (!running_.load()) {
            lost_attempts_cleanup_log->debug("cleanup of {} complete", bucket_name);
            return;
        }
        try {
            handle_atr_cleanup(coll, atr_id);
            std::this_thread::sleep_for(delay);
        } catch (const std::runtime_error& err) {
            lost_attempts_cleanup_log->error("cleanup of atr {} failed with {}, moving on", atr_id, err.what());
        }
    }
    lost_attempts_cleanup_log->info("cleanup of {} complete", bucket_name);
}

const tx::atr_cleanup_stats
tx::transactions_cleanup::handle_atr_cleanup(std::shared_ptr<couchbase::collection> coll,
                                             const std::string& atr_id,
                                             std::vector<transactions_cleanup_attempt>* results)
{
    atr_cleanup_stats stats;
    auto res = coll->exists(atr_id);
    if (res.value->get<bool>()) {
        auto atr = active_transaction_record::get_atr(coll, atr_id);
        if (atr) {
            // ok, loop through the attempts and clean them all.  The entry will
            // check if expired, nothing much to do here except call clean.
            stats.exists = true;
            stats.num_entries = atr->entries().size();
            for (const auto& entry : atr->entries()) {
                // If we were passed results, then we are testing, and want to set the
                // check_if_expired to false.
                atr_cleanup_entry cleanup_entry(entry, coll, *this, results == nullptr);
                try {
                    if (results) {
                        results->emplace_back(cleanup_entry);
                    }
                    cleanup_entry.clean(lost_attempts_cleanup_log, results ? &results->back() : nullptr);
                    if (results) {
                        results->back().success(true);
                    }
                } catch (const std::runtime_error& e) {
                    lost_attempts_cleanup_log->error("cleanup of {} failed: {}, moving on", cleanup_entry, e.what());
                    if (results) {
                        results->back().success(false);
                    }
                }
            }
        }
    }
    lost_attempts_cleanup_log->trace("handle_cleanup_atr {} stats: {}", atr_id, stats.exists, stats.num_entries);
    return stats;
}

void
tx::transactions_cleanup::create_client_record(std::shared_ptr<couchbase::collection> coll)
{
    try {
        couchbase::result res;
        config_.cleanup_hooks().client_record_before_create(coll->bucket_name());
        wrap_collection_call(res, [&](couchbase::result& r) {
            r = coll->mutate_in(CLIENT_RECORD_DOC_ID,
                                { mutate_in_spec::insert(FIELD_CLIENTS, nlohmann::json::object()).create_path().xattr(),
                                  mutate_in_spec::fulldoc_insert(std::string({ 0x00 })) },
                                wrap_option(mutate_in_options(), config_).store_semantics(subdoc_store_semantics::insert));
        });
    } catch (const tx::client_error& e) {
        lost_attempts_cleanup_log->trace("create_client_record got error {}", e.what());
        auto ec = e.ec();
        switch (ec) {
            case FAIL_DOC_ALREADY_EXISTS:
                lost_attempts_cleanup_log->trace("client record already exists, moving on");
                return;
            default:
                throw;
        }
    }
}

const tx::client_record_details
tx::transactions_cleanup::get_active_clients(std::shared_ptr<couchbase::collection> coll, const std::string& uuid)
{
    return retry_op<client_record_details>([&]() -> client_record_details {
        client_record_details details;
        // Write our client record, return details.
        try {
            couchbase::result res;
            config_.cleanup_hooks().client_record_before_get(coll->bucket_name());
            wrap_collection_call(res, [&](couchbase::result& r) {
                r = coll->lookup_in(CLIENT_RECORD_DOC_ID,
                                    { lookup_in_spec::get(FIELD_RECORDS).xattr(), lookup_in_spec::get("$vbucket").xattr() });
            });
            std::vector<std::string> active_client_uids;
            auto hlc = res.values[1].value->get<nlohmann::json>();
            auto now_ms = now_ns_from_vbucket(hlc) / 1000000;
            details.override_enabled = false;
            details.override_expires = 0;
            if (res.values[0].status == 0) {
                auto records = res.values[0].value->get<nlohmann::json>();
                for (auto& r : records.items()) {
                    if (r.key() == FIELD_OVERRIDE) {
                        auto overrides = r.value().get<nlohmann::json>();
                        for (auto& over : overrides.items()) {
                            if (over.key() == FIELD_OVERRIDE_ENABLED) {
                                details.override_enabled = over.value().get<bool>();
                            } else if (over.key() == FIELD_OVERRIDE_EXPIRES) {
                                details.override_expires = over.value().get<uint64_t>();
                            }
                        }
                    } else if (r.key() == FIELD_CLIENTS_ONLY) {
                        auto clients = r.value().get<nlohmann::json>();
                        for (auto& client : clients.items()) {
                            const auto& other_client_uuid = client.key();
                            auto cl = client.value();
                            uint64_t heartbeat_ms = parse_mutation_cas(cl[FIELD_HEARTBEAT].get<std::string>());
                            auto expires_ms = cl[FIELD_EXPIRES].get<uint64_t>();
                            int64_t expired_period = now_ms - heartbeat_ms;
                            bool has_expired = expired_period >= expires_ms && now_ms > heartbeat_ms;
                            lost_attempts_cleanup_log->trace("heartbeat_ms: {}, expires_ms:{}, expired_period: {}, now_ms: {}",
                                                             heartbeat_ms,
                                                             expires_ms,
                                                             expired_period,
                                                             now_ms);
                            if (has_expired && other_client_uuid != uuid) {
                                details.expired_client_ids.push_back(other_client_uuid);
                            } else {
                                active_client_uids.push_back(other_client_uuid);
                            }
                        }
                    }
                }
            }
            if (std::find(active_client_uids.begin(), active_client_uids.end(), uuid) == active_client_uids.end()) {
                active_client_uids.push_back(uuid);
            }
            std::sort(active_client_uids.begin(), active_client_uids.end());
            size_t this_idx =
              std::distance(active_client_uids.begin(), std::find(active_client_uids.begin(), active_client_uids.end(), uuid));
            details.num_active_clients = active_client_uids.size();
            details.index_of_this_client = this_idx;
            details.num_active_clients = active_client_uids.size();
            details.num_expired_clients = details.expired_client_ids.size();
            details.num_existing_clients = details.num_expired_clients + details.num_active_clients;
            details.client_uuid = uuid;
            details.cas_now_nanos = now_ms * 1000000;
            std::vector<mutate_in_spec> specs;
            details.override_active = (details.override_enabled && details.override_expires > details.cas_now_nanos);
            lost_attempts_cleanup_log->trace("details {}", details);
            if (details.override_active) {
                lost_attempts_cleanup_log->trace("override enabled, will not update record");
                return details;
            }
            specs.push_back(mutate_in_spec::upsert(FIELD_CLIENTS + "." + uuid + "." + FIELD_HEARTBEAT, "${Mutation.CAS}")
                              .xattr()
                              .create_path()
                              .expand_macro());
            specs.push_back(mutate_in_spec::upsert(FIELD_CLIENTS + "." + uuid + "." + FIELD_EXPIRES,
                                                   config_.cleanup_window().count() / 2 + SAFETY_MARGIN_EXPIRY_MS)
                              .xattr()
                              .create_path());
            for (auto idx = 0; idx < std::min(details.expired_client_ids.size(), (size_t)13); idx++) {
                specs.push_back(mutate_in_spec::remove(FIELD_CLIENTS + "." + details.expired_client_ids[idx]).xattr());
            }
            config_.cleanup_hooks().client_record_before_update(coll->bucket_name());
            wrap_collection_call(res, [&](couchbase::result& r) {
                r = coll->mutate_in(CLIENT_RECORD_DOC_ID, specs, wrap_option(mutate_in_options(), config_));
            });
            // just update the cas, and return the details
            details.cas_now_nanos = res.cas;
            lost_attempts_cleanup_log->debug("get_active_clients found {}", details);
            return details;
        } catch (const tx::client_error& e) {
            auto ec = e.ec();
            switch (ec) {
                case FAIL_DOC_NOT_FOUND:
                    lost_attempts_cleanup_log->debug("client record not found, creating new one");
                    create_client_record(coll);
                    throw retry_operation("Client record didn't exist. Creating and retrying");
                default:
                    throw;
            }
        }
    });
}

void
tx::transactions_cleanup::remove_client_record_from_all_buckets(const std::string& uuid)
{
    for (auto bucket_name : cluster_.buckets()) {
        try {
            retry_op_exponential_backoff_timeout<void>(
              std::chrono::milliseconds(10), std::chrono::milliseconds(250), std::chrono::milliseconds(500), [&]() {
                  try {
                      auto coll = cluster_.bucket(bucket_name)->default_collection();
                      // insure a client record document exists...
                      create_client_record(coll);
                      // now, proceed to remove the client uuid if it exists
                      config_.cleanup_hooks().client_record_before_remove_client(bucket_name);
                      couchbase::result res;
                      wrap_collection_call(res, [&](couchbase::result& r) {
                          r = coll->mutate_in(CLIENT_RECORD_DOC_ID,
                                              { mutate_in_spec::upsert(FIELD_CLIENTS + "." + uuid, nullptr).xattr(),
                                                mutate_in_spec::remove(FIELD_CLIENTS + "." + uuid).xattr() },
                                              wrap_option(mutate_in_options(), config_));
                      });
                      lost_attempts_cleanup_log->debug("removed {} from {}", uuid, bucket_name);
                  } catch (const tx::client_error& e) {
                      lost_attempts_cleanup_log->debug("error removing client records {}", e.what());
                      auto ec = e.ec();
                      switch (ec) {
                          case FAIL_DOC_NOT_FOUND:
                              lost_attempts_cleanup_log->debug("no client record in {}, ignoring", bucket_name);
                              return;
                          case FAIL_PATH_NOT_FOUND:
                              lost_attempts_cleanup_log->debug("client {} not in client record for {}, ignoring", uuid, bucket_name);
                              return;
                          default:
                              throw retry_operation("retry remove until timeout");
                      }
                  }
              });
        } catch (const std::exception& e) {
            lost_attempts_cleanup_log->error("Error removing client record {} from bucket {}", uuid, bucket_name);
        }
    }
}

void
tx::transactions_cleanup::lost_attempts_loop()
{
    lost_attempts_cleanup_log->info("starting lost attempts loop");
    while (running_.load()) {
        std::list<std::thread> workers;
        try {
            auto names = cluster_.buckets();
            lost_attempts_cleanup_log->info("creating {} tasks to clean buckets", names.size());
            // TODO consider std::async here.
            for (const auto& name : names) {
                workers.emplace_back([&]() {
                    try {
                        clean_lost_attempts_in_bucket(name);
                    } catch (const std::runtime_error& e) {
                        lost_attempts_cleanup_log->error("got error {} attempting to clean {}", e.what(), name);
                    }
                });
            }
            for (auto& thr : workers) {
                if (thr.joinable()) {
                    thr.join();
                }
            }
        } catch (const std::exception& e) {
            lost_attempts_cleanup_log->error("got error {}, rescheduling in {}ms", e.what(), config_.cleanup_window().count());
            interruptable_wait(config_.cleanup_window());
        }
    }
    remove_client_record_from_all_buckets(client_uuid_);
}

const tx::atr_cleanup_stats
tx::transactions_cleanup::force_cleanup_atr(std::shared_ptr<couchbase::collection> coll,
                                            const std::string& atr_id,
                                            std::vector<transactions_cleanup_attempt>& results)
{
    lost_attempts_cleanup_log->trace("starting force_cleanup_atr coll: {} atr_id {}", coll->name(), atr_id);
    return handle_atr_cleanup(coll, atr_id, &results);
}

void
tx::transactions_cleanup::force_cleanup_entry(atr_cleanup_entry& entry, transactions_cleanup_attempt& attempt)
{
    try {
        entry.clean(attempt_cleanup_log, &attempt);
        attempt.success(true);

    } catch (const std::runtime_error& e) {
        attempt_cleanup_log->error("error attempting to clean {}: {}", entry, e.what());
        attempt.success(false);
    }
}

void
tx::transactions_cleanup::force_cleanup_attempts(std::vector<transactions_cleanup_attempt>& results)
{
    attempt_cleanup_log->trace("starting force_cleanup_attempts");
    while (atr_queue_.size() > 0) {
        auto entry = atr_queue_.pop(false);
        if (!entry) {
            attempt_cleanup_log->error("pop failed to return entry, but queue size {}", atr_queue_.size());
            return;
        }
        results.emplace_back(*entry);
        try {
            entry->clean(attempt_cleanup_log, &results.back());
            results.back().success(true);
        } catch (std::runtime_error& e) {
            results.back().success(false);
        }
    }
}

void
tx::transactions_cleanup::attempts_loop()
{
    try {
        attempt_cleanup_log->debug("cleanup attempts loop starting...");
        while (interruptable_wait(cleanup_loop_delay_)) {
            while (auto entry = atr_queue_.pop()) {
                if (!running_.load()) {
                    attempt_cleanup_log->debug("loop stopping - {} entries on queue", atr_queue_.size());
                    return;
                }
                if (entry) {
                    attempt_cleanup_log->trace("beginning cleanup on {}", *entry);
                    try {
                        entry->clean(attempt_cleanup_log);
                    } catch (...) {
                        // catch everything as we don't want to raise out of this thread
                        attempt_cleanup_log->info("got error cleaning {}, leaving for lost txn cleanup", entry);
                    }
                }
            }
        }
        attempt_cleanup_log->info("stopping - {} entries on queue", atr_queue_.size());
    } catch (const std::runtime_error& e) {
        attempt_cleanup_log->error("got error {} in attempts_loop", e.what());
    }
}

void
tx::transactions_cleanup::add_attempt(attempt_context& ctx)
{
    auto& ctx_impl = static_cast<attempt_context_impl&>(ctx);
    switch (ctx_impl.state()) {
        case tx::attempt_state::NOT_STARTED:
        case tx::attempt_state::COMPLETED:
        case tx::attempt_state::ROLLED_BACK:
            attempt_cleanup_log->trace("attempt in state {}, not adding to cleanup", tx::attempt_state_name(ctx_impl.state()));
            return;
        default:
            if (config_.cleanup_client_attempts()) {
                attempt_cleanup_log->debug("adding attempt {} to cleanup queue", ctx_impl.id());
                atr_queue_.push(ctx);
            } else {
                attempt_cleanup_log->trace("not cleaning client attempts, ignoring {}", ctx_impl.id());
            }
    }
}

void
tx::transactions_cleanup::close()
{
    {
        std::unique_lock<std::mutex> lock(mutex_);
        running_ = false;
        cv_.notify_all();
    }
    if (cleanup_thr_.joinable()) {
        cleanup_thr_.join();
        attempt_cleanup_log->info("cleanup attempt thread closed");
    }
    if (lost_attempts_thr_.joinable()) {
        lost_attempts_thr_.join();
        lost_attempts_cleanup_log->info("lost attempts thread closed");
    }
}

tx::transactions_cleanup::~transactions_cleanup()
{
    close();
}
