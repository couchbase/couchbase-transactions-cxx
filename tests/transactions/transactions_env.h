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

#pragma once
#include "../../src/transactions/result.hxx"
#include "../../src/transactions/uid_generator.hxx"
#include "couchbase/transactions/internal/utils.hxx"
#include <couchbase/transactions.hxx>

#include <couchbase/cluster.hxx>
#include <couchbase/operations.hxx>
#include <couchbase/support.hxx>

#include <couchbase/internal/nlohmann/json.hpp>
#include <cstdlib>
#include <fstream>
#include <gtest/gtest.h>
#include <spdlog/fmt/ostr.h>
#include <spdlog/spdlog.h>

// hack, until I get gtest working a bit better and can execute
// tests through make with proper working directory.
#define CONFIG_FILE_NAME "../tests/config.json"
#define ENV_CONNECTION_STRING "TXN_CONNECTION_STRING"
static const uint32_t DEFAULT_IO_COMPLETION_THREADS = 4;
static const size_t MAX_PINGS = 10;
static const auto PING_INTERVAL = std::chrono::milliseconds(100);

namespace tx = couchbase::transactions;

struct conn {
    asio::io_context io;
    std::list<std::thread> io_threads;
    std::shared_ptr<couchbase::cluster> c;

    conn(const nlohmann::json& conf)
      : io({})
      , c(couchbase::cluster::create(io))
    {
        // for tests, really chatty logs may be useful.
        if (!couchbase::logger::is_initialized()) {
            couchbase::logger::create_console_logger();
        }
        couchbase::logger::set_log_levels(couchbase::logger::level::trace);
        couchbase::transactions::set_transactions_log_level(couchbase::logger::level::trace);
        size_t num_threads = conf.contains("io_threads") ? conf["io_threads"].get<uint32_t>() : 4;
        couchbase::transactions::txn_log->trace("using {} io completion threads", num_threads);
        for (size_t i = 0; i < num_threads; i++) {
            io_threads.emplace_back([this]() { io.run(); });
        }
        connect(conf);
    }
    ~conn()
    {
        // close connection
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        c->close([barrier]() { barrier->set_value(); });
        f.get();
        for (auto& t : io_threads) {
            if (t.joinable()) {
                t.join();
            } else {
                couchbase::transactions::txn_log->warn("io completion thread not joinable");
            }
        }
    }

    void connect(const nlohmann::json& conf)
    {
        couchbase::cluster_credentials auth{};
        {
            if (auto env_val = std::getenv("COUCHBASE_CXX_CLIENT_LOG_LEVEL")) {
                couchbase::logger::set_log_levels(couchbase::logger::level_from_str(env_val));
            }
            auto connstr = couchbase::utils::parse_connection_string(conf["connection_string"]);
            auth.username = "Administrator";
            auth.password = "password";
            auto barrier = std::make_shared<std::promise<std::error_code>>();
            auto f = barrier->get_future();
            c->open(couchbase::origin(auth, connstr), [barrier](std::error_code ec) { barrier->set_value(ec); });
            auto rc = f.get();
            if (rc) {
                std::cout << "ERROR opening cluster: " << rc.message() << std::endl;
                exit(-1);
            }
            spdlog::trace("successfully opened connection to {}", connstr.bootstrap_nodes.front().address);
        }
        // now, open the `default` bucket
        {
            auto barrier = std::make_shared<std::promise<std::error_code>>();
            auto f = barrier->get_future();
            c->open_bucket("default", [barrier](std::error_code ec) { barrier->set_value(ec); });
            auto rc = f.get();
            if (rc) {
                std::cout << "ERROR opening bucket `default`: " << rc.message() << std::endl;
                exit(-1);
            }
        }

        // do a ping
        {
            bool ok = false;
            size_t num_pings = 0;
            auto sleep_time = PING_INTERVAL;
            // TEMPORARILY: because of CCXCBC-94, we can only sleep for some arbitrary time before pinging,
            // in hopes that query is up by then.
            spdlog::info("sleeping for 10 seconds before pinging (CXXCBC-94 workaround/hack)");
            std::this_thread::sleep_for(std::chrono::seconds(10));
            while (!ok && num_pings++ < MAX_PINGS) {
                spdlog::info("sleeping {}ms before pinging...", sleep_time.count());
                std::this_thread::sleep_for(std::chrono::milliseconds(sleep_time));
                // TEMPORARILY only ping key_value.   See CCXCBC-94 for details -- ping not returning any service
                // except KV.
                std::set<couchbase::service_type> services{ couchbase::service_type::key_value };
                auto barrier = std::make_shared<std::promise<couchbase::diag::ping_result>>();
                auto f = barrier->get_future();
                c->ping(
                  "tests_startup", "default", services, [barrier](couchbase::diag::ping_result result) { barrier->set_value(result); });
                auto result = f.get();
                ok = true;
                for (auto& svc : services) {
                    if (result.services.find(svc) != result.services.end()) {
                        if (result.services[svc].size() > 0) {
                            ok = ok && std::all_of(result.services[svc].begin(),
                                                   result.services[svc].end(),
                                                   [&](const couchbase::diag::endpoint_ping_info& info) {
                                                       return (!info.error && info.state == couchbase::diag::ping_state::ok);
                                                   });
                        } else {
                            ok = false;
                        }
                    } else {
                        ok = false;
                    }
                }
                spdlog::info("ping after connect {}", ok ? "successful" : "unsuccessful");
            }
            if (!ok) {
                exit(-1);
            }
        }
    }
};

class TransactionsTestEnvironment : public ::testing::Environment
{
  public:
    void SetUp() override
    {
        get_cluster();
    }

    static bool supports_query()
    {
        return nullptr != std::getenv("SUPPORTS_QUERY");
    }

    static const nlohmann::json& get_conf()
    {
        // read config.json
        static nlohmann::json conf;
        if (conf.empty()) {
            spdlog::info("reading config file {}", CONFIG_FILE_NAME);
            std::ifstream in(CONFIG_FILE_NAME, std::ifstream::in);
            conf = nlohmann::json::parse(in);
            char* override_conn_str = std::getenv(ENV_CONNECTION_STRING);
            if (override_conn_str) {
                spdlog::trace("overriding connection string - '{}'", override_conn_str);
                conf["connection_string"] = override_conn_str;
            }
        }
        return conf;
    }

    static bool upsert_doc(const couchbase::document_id& id, const std::string& content)
    {
        auto& c = get_cluster();
        couchbase::operations::upsert_request req{ id };
        req.value = content;
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        c.execute(req, [barrier](couchbase::operations::upsert_response resp) { barrier->set_value(resp.ctx.ec); });
        auto ec = f.get();
        if (ec) {
            spdlog::error("upsert doc failed with {}", ec.message());
        }
        return !ec;
    }

    static bool insert_doc(const couchbase::document_id& id, const std::string& content)
    {
        auto& c = get_cluster();
        couchbase::operations::insert_request req{ id };
        req.value = content;
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        c.execute(req, [barrier](couchbase::operations::insert_response resp) { barrier->set_value(resp.ctx.ec); });
        auto ec = f.get();
        if (ec) {
            spdlog::error("insert doc failed with {}", ec.message());
        }
        return !ec;
    }

    static tx::result get_doc(const couchbase::document_id& id)
    {
        auto& c = get_cluster();
        couchbase::operations::get_request req{ id };
        auto barrier = std::make_shared<std::promise<tx::result>>();
        auto f = barrier->get_future();
        c.execute(req, [barrier](couchbase::operations::get_response resp) { barrier->set_value(tx::result::create_from_response(resp)); });
        return tx::wrap_operation_future(f);
    }

    static couchbase::cluster& get_cluster()
    {
        static conn connection(get_conf());
        return *connection.c;
    }

    static couchbase::document_id get_document_id(const std::string& id = {})
    {
        std::string key = (id.empty() ? couchbase::transactions::uid_generator::next() : id);
        return { "default", "_default", "_default", key };
    }

    static couchbase::transactions::transactions get_transactions(couchbase::cluster& c = get_cluster(),
                                                                  bool cleanup_client_attempts = false,
                                                                  bool cleanup_lost_txns = false)
    {
        couchbase::transactions::transaction_config cfg;
        cfg.cleanup_client_attempts(true);
        cfg.cleanup_lost_attempts(true);
        cfg.expiration_time(std::chrono::seconds(5));
        return { c, cfg };
    }
};
