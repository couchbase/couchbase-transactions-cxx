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
#include "../../src/transactions/uid_generator.hxx"
#include "../../src/transactions/utils.hxx"
#include <couchbase/transactions.hxx>
#include <couchbase/transactions/result.hxx>

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

namespace tx = couchbase::transactions;

struct conn {
    asio::io_context io;
    std::thread io_thread;
    couchbase::cluster c;

    conn(const nlohmann::json& conf)
      : io({})
      , c(io)
    {
        io_thread = std::thread([this]() { io.run(); });
        connect(conf);
    }
    ~conn()
    {
        // close connection
        auto barrier = std::make_shared<std::promise<void>>();
        auto f = barrier->get_future();
        c.close([barrier]() { barrier->set_value(); });
        f.get();
        io_thread.join();
    }

    void connect(const nlohmann::json& conf)
    {
        couchbase::cluster_credentials auth{};
        {
            auto connstr = couchbase::utils::parse_connection_string(conf["connection_string"]);
            auth.username = "Administrator";
            auth.password = "password";
            auto barrier = std::make_shared<std::promise<std::error_code>>();
            auto f = barrier->get_future();
            c.open(couchbase::origin(auth, connstr), [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
            auto rc = f.get();
            if (rc) {
                std::cout << "ERROR opening cluster: " << rc.message() << std::endl;
                exit(-1);
            }
        }
        // now, open the `default` bucket
        {
            auto barrier = std::make_shared<std::promise<std::error_code>>();
            auto f = barrier->get_future();
            c.open_bucket("default", [barrier](std::error_code ec) mutable { barrier->set_value(ec); });
            auto rc = f.get();
            if (rc) {
                std::cout << "ERROR opening bucket `default`: " << rc.message() << std::endl;
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
        // for tests, really chatty logs may be useful.
        couchbase::transactions::set_transactions_log_level(couchbase::transactions::log_level::TRACE);
        get_cluster();
    }

    static bool supports_collections()
    {
        return nullptr != std::getenv("SUPPORTS_COLLECTIONS");
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
        c.execute(req, [barrier](couchbase::operations::upsert_response resp) mutable { barrier->set_value(resp.ctx.ec); });
        auto ec = f.get();
        return !ec;
    }

    static bool insert_doc(const couchbase::document_id& id, const std::string& content)
    {
        auto& c = get_cluster();
        couchbase::operations::insert_request req{ id };
        req.value = content;
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        c.execute(req, [barrier](couchbase::operations::insert_response resp) mutable { barrier->set_value(resp.ctx.ec); });
        auto ec = f.get();
        return !ec;
    }

    static tx::result get_doc(const couchbase::document_id& id)
    {
        auto& c = get_cluster();
        couchbase::operations::get_request req{ id };
        auto barrier = std::make_shared<std::promise<tx::result>>();
        auto f = barrier->get_future();
        c.execute(
          req, [barrier](couchbase::operations::get_response resp) mutable { barrier->set_value(tx::result::create_from_response(resp)); });
        return tx::wrap_operation_future(f);
    }

    static couchbase::cluster& get_cluster()
    {
        static conn connection(get_conf());
        return connection.c;
    }

    static couchbase::document_id get_document_id(const std::string& id = {})
    {
        std::string key = (id.empty() ? couchbase::transactions::uid_generator::next() : id);
        return { "default", "_default", "_default", key };
    }
};
