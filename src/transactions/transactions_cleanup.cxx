#include <chrono>
#include <functional>
#include <iostream>

#include <spdlog/fmt/ostr.h>
#include <unistd.h>

#include <couchbase/client/cluster.hxx>
#include <couchbase/client/collection.hxx>
#include <couchbase/transactions/client_record.hxx>
#include <couchbase/transactions/transaction_fields.hxx>
#include <couchbase/transactions/transactions_cleanup.hxx>

#include "atr_ids.hxx"
#include "logging.hxx"
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
static const std::string FIELD_CLIENTS = FIELD_RECORDS + ".clients";
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
    spdlog::trace("lost attempts cleanup for {} starting", bucket_name);
    if (!running_.load()) {
        spdlog::trace("lost attempts cleanup of {} complete", bucket_name);
        return;
    }
    // each thread needs its own cluster, copy cluster_
    auto c = cluster_;
    auto coll = c.bucket(bucket_name)->default_collection();
    auto details = get_active_clients(coll, client_uuid_);
    auto all_atrs = atr_ids::all();
    spdlog::trace("found {} other active clients", details.num_active_clients);
    for (auto it = all_atrs.begin() + details.index_of_this_client; it < all_atrs.end(); it += details.num_active_clients) {
        // clean the ATR entry
        std::string atr_id = *it;
        if (!running_.load()) {
            spdlog::trace("lost attempts cleanup of {} complete", bucket_name);
            return;
        }
        try {
            handle_atr_cleanup(coll, atr_id);
        } catch (const std::runtime_error& err) {
            spdlog::error("cleanup of atr {} failed with {}, moving on", atr_id, err.what());
        }
    }
    spdlog::trace("lost attempts cleanup of {} complete", bucket_name);
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
                    cleanup_entry.clean(results ? &results->back() : nullptr);
                    if (results) {
                        results->back().success(true);
                    }
                } catch (const std::runtime_error& e) {
                    spdlog::error("cleanup of {} failed: {}, moving on", cleanup_entry, e.what());
                    if (results) {
                        results->back().success(false);
                    }
                }
            }
        }
    }
    spdlog::trace("handle_cleanup_atr {} stats: {}", atr_id, stats.exists, stats.num_entries);
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
                                { mutate_in_spec::insert(FIELD_CLIENTS, nlohmann::json::object()).create_path().xattr() },
                                mutate_in_options().store_semantics(subdoc_store_semantics::insert));
        });
    } catch (const tx::client_error& e) {
        spdlog::trace("create_client_record got error {}", e.what());
        auto ec = e.ec();
        switch (ec) {
            case FAIL_DOC_ALREADY_EXISTS:
                spdlog::trace("client record already exists, moving on");
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
        // Write our cient record, return offset, increment to use
        try {
            couchbase::result res;
            config_.cleanup_hooks().client_record_before_get(coll->bucket_name());
            wrap_collection_call(res, [&](couchbase::result& r) {
                r = coll->lookup_in(CLIENT_RECORD_DOC_ID,
                                    { lookup_in_spec::get(FIELD_CLIENTS).xattr(),
                                      lookup_in_spec::get(FIELD_RECORDS + "." + FIELD_OVERRIDE + "." + FIELD_OVERRIDE_ENABLED).xattr(),
                                      lookup_in_spec::get(FIELD_RECORDS + "." + FIELD_OVERRIDE + "." + FIELD_OVERRIDE_EXPIRES).xattr() });
            });
            std::vector<std::string> active_client_uids;
            if (res.values[0].status == 0) {
                for (auto& client : res.values[0].value->items()) {
                    const auto& other_client_uuid = client.key();
                    auto cl = client.value();
                    uint64_t cas_ms = res.cas / 1000000;
                    uint64_t heartbeat_ms = parse_mutation_cas(cl[FIELD_HEARTBEAT].get<std::string>());
                    auto expires_ms = cl[FIELD_EXPIRES].get<uint64_t>();
                    uint64_t expired_period = cas_ms - heartbeat_ms;
                    bool has_expired = expired_period >= expires_ms;
                    if (has_expired && other_client_uuid != uuid) {
                        details.expired_client_ids.push_back(other_client_uuid);
                    } else {
                        active_client_uids.push_back(other_client_uuid);
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
            details.cas_now_nanos = res.cas;
            std::vector<mutate_in_spec> specs;
            details.override_enabled = false;
            details.override_expires = 0;
            if (res.values[1].status == 0) {
                details.override_enabled = res.values[1].value->get<bool>();
            }
            if (res.values[2].status == 0) {
                details.override_expires = res.values[2].value->get<uint64_t>();
            }
            details.override_active = (details.override_enabled && details.override_expires > details.cas_now_nanos);
            spdlog::trace("details {}", details);
            if (details.override_active) {
                spdlog::trace("override enabled, will not update record");
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
            wrap_collection_call(res, [&](couchbase::result& r) { r = coll->mutate_in(CLIENT_RECORD_DOC_ID, specs); });
            // just update the cas, and return the details
            details.cas_now_nanos = res.cas;
            return details;
        } catch (const tx::client_error& e) {
            auto ec = e.ec();
            switch (ec) {
                case FAIL_DOC_NOT_FOUND:
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
    // using <void> forces me to specialize the template for retry_op, perhaps
    // not worth it.  using <int> since it is convenient.
    int buckets = 0;
    for (auto bucket_name : cluster_.buckets()) {
        buckets++;
        retry_op<int>([&]() {
            try {
                auto coll = cluster_.bucket(bucket_name)->default_collection();
                config_.cleanup_hooks().client_record_before_remove_client(bucket_name);
                couchbase::result res;
                wrap_collection_call(res, [&](couchbase::result& r) {
                    r = coll->mutate_in(CLIENT_RECORD_DOC_ID, { mutate_in_spec::remove(FIELD_CLIENTS + "." + uuid).xattr() });
                });
                spdlog::info("removed {} from {}", uuid, bucket_name);
                return buckets;
            } catch (const tx::client_error& e) {
                spdlog::trace("error removing client records {}", e.what());
                auto ec = e.ec();
                switch (ec) {
                    case FAIL_DOC_NOT_FOUND:
                        spdlog::trace("no client record in {}, ignoring", bucket_name);
                        return buckets;
                    case FAIL_PATH_NOT_FOUND:
                        spdlog::trace("client {} not in client record for {}, ignoring", uuid, bucket_name);
                        return buckets;
                    default:
                        throw retry_operation("retry remove until timeout");
                }
            }
        });
    }
}

void
tx::transactions_cleanup::lost_attempts_loop()
{
    try {
        spdlog::info("starting lost attempts loop");
        while (interruptable_wait(config_.cleanup_window())) {
            auto names = cluster_.buckets();
            spdlog::info("creating {} tasks to clean buckets", names.size());
            // TODO consider std::async here.
            std::list<std::thread> workers;
            for (const auto& name : names) {
                workers.emplace_back([&]() {
                    try {
                        clean_lost_attempts_in_bucket(name);
                    } catch (const std::runtime_error& e) {
                        spdlog::error("got error {} attempting to clean {}", e.what(), name);
                    }
                });
            }
            for (auto& thr : workers) {
                if (thr.joinable()) {
                    thr.join();
                }
            }
            spdlog::info("lost txn loops complete, scheduled to run again in {} ms", config_.cleanup_window().count());
        }
    } catch (const std::runtime_error& e) {
        spdlog::error("got error {} in lost attempts loop", e.what());
    }
}

const tx::atr_cleanup_stats
tx::transactions_cleanup::force_cleanup_atr(std::shared_ptr<couchbase::collection> coll,
                                            const std::string& atr_id,
                                            std::vector<transactions_cleanup_attempt>& results)
{
    spdlog::trace("starting force_cleanup_atr coll: {} atr_id {}", coll->name(), atr_id);
    return handle_atr_cleanup(coll, atr_id, &results);
}

void
tx::transactions_cleanup::force_cleanup_entry(atr_cleanup_entry& entry, transactions_cleanup_attempt& attempt)
{
    try {
        entry.clean(&attempt);
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
            entry->clean(&results.back());
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
        spdlog::info("cleanup attempts loop starting...");
        while (interruptable_wait(cleanup_loop_delay_)) {
            while (auto entry = atr_queue_.pop()) {
                if (!running_.load()) {
                    spdlog::info("attempts_loop stopping - {} entries on queue", atr_queue_.size());
                    return;
                }
                if (entry) {
                    spdlog::trace("beginning cleanup on {}", *entry);
                    try {
                        entry->clean();
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
            }
        }
        spdlog::info("attempts_loop stopping - {} entries on queue", atr_queue_.size());
    } catch (const std::runtime_error& e) {
        spdlog::error("got error {} in attempts_loop", e.what());
    }
}

void
tx::transactions_cleanup::add_attempt(attempt_context& ctx)
{
    if (ctx.state() == tx::attempt_state::NOT_STARTED) {
        spdlog::trace("attempt not started, not adding to cleanup");
        return;
    }
    if (config_.cleanup_client_attempts()) {
        spdlog::trace("adding attempt {} to cleanup queue", ctx.id());
        atr_queue_.push(ctx);
    } else {
        spdlog::trace("not cleaning client attempts, ignoring {}", ctx.id());
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
        spdlog::info("cleanup attempt thread closed");
    }
    if (lost_attempts_thr_.joinable()) {
        lost_attempts_thr_.join();
        spdlog::info("lost attempts thread closed");
    }
}

tx::transactions_cleanup::~transactions_cleanup()
{
    close();
}
