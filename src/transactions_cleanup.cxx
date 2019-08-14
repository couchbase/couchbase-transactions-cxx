#include <functional>
#include <iostream>
#include <chrono>

#include <unistd.h>

#include <libcouchbase/transactions/transactions_cleanup.hxx>
#include <libcouchbase/transactions/client_record.hxx>
#include <libcouchbase/transactions/uid_generator.hxx>
#include <libcouchbase/transactions/transaction_fields.hxx>

#include "atr_ids.hxx"

couchbase::transactions::transactions_cleanup::transactions_cleanup(couchbase::cluster &cluster,
                                                                    const couchbase::transactions::configuration &config)
    : cluster_(cluster), config_(config), client_uuid_(uid_generator::next())
{
    lost_attempts_thr = std::thread(std::bind(&transactions_cleanup::lost_attempts_loop, this));
}

static uint64_t byteswap64(uint64_t val)
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
static uint64_t parse_mutation_cas(std::string cas)
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

void couchbase::transactions::transactions_cleanup::lost_attempts_loop()
{
    auto names = cluster_.buckets();
    std::list<std::thread> workers;
    for (const auto &name : names) {
        auto bkt = cluster_.open_bucket(name);
        auto uid = client_uuid_;
        auto config = config_;
        workers.emplace_back([bkt = std::move(bkt), uid, config, running = &running]() {
            auto col = bkt->default_collection();
            while (*running) {
                col->mutate_in(CLIENT_RECORD_DOC_ID, {
                                                         mutate_in_spec::upsert("dummy", nullptr).xattr(),
                                                     });
                auto res = col->lookup_in(CLIENT_RECORD_DOC_ID, { lookup_in_spec::get(FIELD_CLIENTS).xattr() });
                std::vector<std::string> expired_client_uids;
                std::vector<std::string> active_client_uids;
                for (auto &client : res.values[0].object_items()) {
                    auto &other_client_uid = client.first;
                    auto cl = client.second;
                    uint64_t cas_ms = res.cas / 1000000;
                    uint64_t heartbeat_ms = parse_mutation_cas(cl[FIELD_HEARTBEAT].string_value());
                    uint64_t expires_ms = cl[FIELD_EXPIRES].int_value();
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
                                                       config.cleanup_window() / 2 + SAFETY_MARGIN_EXPIRY_MS)
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
                    for (auto &kv : atr.values[0].object_items()) {
                        auto attempt_id = kv.first;
                        auto entry = kv.second;
                        std::string status = entry[ATR_FIELD_STATUS].string_value();
                        uint64_t start_ms = parse_mutation_cas(entry[ATR_FIELD_START_TIMESTAMP].string_value());
                        uint64_t commit_ms = parse_mutation_cas(entry[ATR_FIELD_START_COMMIT].string_value());
                        uint64_t complete_ms = parse_mutation_cas(entry[ATR_FIELD_TIMESTAMP_COMPLETE].string_value());
                        uint64_t rollback_ms = parse_mutation_cas(entry[ATR_FIELD_TIMESTAMP_ROLLBACK_START].string_value());
                        uint64_t rolledback_ms = parse_mutation_cas(entry[ATR_FIELD_TIMESTAMP_ROLLBACK_COMPLETE].string_value());
                        int expires_after_ms = entry[ATR_FIELD_EXPIRES_AFTER_MSECS].int_value();
                        auto inserted_ids = entry[ATR_FIELD_DOCS_INSERTED].array_items();
                        auto replaced_ids = entry[ATR_FIELD_DOCS_REPLACED].array_items();
                        auto removed_ids = entry[ATR_FIELD_DOCS_REMOVED].array_items();

                        const uint64_t safety_margin_ms = 2500;
                        bool has_expired = false;
                        if (start_ms > 0) {
                            has_expired = (cas_ms - start_ms) > (expires_after_ms + safety_margin_ms);
                        }
                        if (!has_expired) {
                            continue;
                        }
                        if (status == "COMMITTED") {
                            for (auto &id : inserted_ids) {
                                result doc =
                                    col->lookup_in(id.string_value(), {
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

                using namespace std::chrono_literals;
                std::this_thread::sleep_for(100ms);
            }
        });
    }
    for (auto &thr : workers) {
        thr.join();
    }
}

couchbase::transactions::transactions_cleanup::~transactions_cleanup()
{
    running = false;
    lost_attempts_thr.join();
}
