#include "../client/client_env.h"
#include <couchbase/client/cluster.hxx>
#include <couchbase/client/collection.hxx>
#include <couchbase/transactions.hxx>
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <stdexcept>

using namespace couchbase::transactions;

auto content = nlohmann::json::parse("{\"some\": \"thing\"}");

TEST(SimpleTransactions, ArbitraryRuntimeError)
{
    auto cluster = ClientTestEnvironment::get_cluster();
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    ::couchbase::transactions::transactions txn(*cluster, cfg);
    auto coll = cluster->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    EXPECT_THROW(
      {
          try {
              txn.run([&](attempt_context& ctx) {
                  ctx.insert(coll, id, content);
                  throw std::runtime_error("Yo");
              });
          } catch (const transaction_failed& e) {
              EXPECT_EQ(e.cause(), external_exception::UNKNOWN);
              EXPECT_STREQ("Yo", e.what());
              throw;
          }
      },
      transaction_failed);
}

TEST(SimpleTransactions, ArbitraryException)
{
    auto cluster = ClientTestEnvironment::get_cluster();
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    ::couchbase::transactions::transactions txn(*cluster, cfg);
    auto coll = cluster->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    EXPECT_THROW(
      {
          try {
              txn.run([&](attempt_context& ctx) {
                  ctx.insert(coll, id, content);
                  throw 3;
              });
          } catch (const transaction_failed& e) {
              EXPECT_EQ(e.cause(), external_exception::UNKNOWN);
              EXPECT_STREQ("Unexpected error", e.what());
              throw;
          }
      },
      transaction_failed);
}
