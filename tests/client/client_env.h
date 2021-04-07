#pragma once
#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>
#include <couchbase/client/cluster.hxx>
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

using namespace couchbase;

class ClientTestEnvironment : public ::testing::Environment
{
  public:
    void SetUp() override
    {
        // for tests, really chatty logs may be useful.
        set_client_log_level(log_level::TRACE);
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

    static std::shared_ptr<cluster> get_cluster(const cluster_options& opts = cluster_options())
    {
        auto conf = get_conf();
        static auto c = std::make_shared<cluster>(conf["connection_string"], conf["username"], conf["password"], opts);
        return c;
    }

    static std::string get_uuid()
    {
        static auto generator = boost::uuids::random_generator();
        return boost::uuids::to_string(generator());
    }

    static std::shared_ptr<collection> get_default_collection(const std::string& bucket_name)
    {
        auto c = get_cluster();
        auto b = c->bucket(bucket_name);
        return b->default_collection();
    }
};
