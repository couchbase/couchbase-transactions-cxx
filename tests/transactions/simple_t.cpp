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

#include "../client/client_env.h"
#include "../client/helpers.hxx"
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

TEST(SimpleTransactions, CanGetReplace)
{
    auto cluster = ClientTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    couchbase::transactions::transactions txn(*cluster, cfg);

    // upsert initial doc
    auto coll = cluster->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    ASSERT_TRUE(coll->upsert(id, c).is_success());
    txn.run([&](attempt_context& ctx) {
        auto doc = ctx.get(coll, id);
        auto content = doc.content<nlohmann::json>();
        content["another one"] = 1;
        ctx.replace(coll, doc, content);
    });
    // now add to the original content, and compare
    c["another one"] = 1;
    ASSERT_EQ(c, coll->get(id).content_as<nlohmann::json>());
}

TEST(SimpleTransactions, CanGetReplaceRawStrings)
{
    auto cluster = ClientTestEnvironment::get_cluster();
    auto c = nlohmann::json::parse("{\"some number\": 0}");
    std::string new_content("{\"aaa\":\"bbb\"}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    couchbase::transactions::transactions txn(*cluster, cfg);

    // upsert initial doc
    auto coll = cluster->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    ASSERT_TRUE(coll->upsert(id, c).is_success());
    txn.run([&](attempt_context& ctx) {
        auto doc = ctx.get(coll, id);
        ctx.replace(coll, doc, new_content);
    });
    ASSERT_EQ(new_content, coll->get(id).content_as<std::string>());
}

TEST(SimpleTransactions, CanGetReplaceObjects)
{
    auto cluster = ClientTestEnvironment::get_cluster();
    SimpleObject o{ "someone", 100 };
    SimpleObject o2{ "someone else", 200 };
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    couchbase::transactions::transactions txn(*cluster, cfg);

    // upsert initial doc
    auto coll = cluster->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    ASSERT_TRUE(coll->upsert(id, o).is_success());
    txn.run([&](attempt_context& ctx) {
        auto doc = ctx.get(coll, id);
        ctx.replace(coll, doc, o2);
    });
    ASSERT_EQ(o2, coll->get(id).content_as<SimpleObject>());
}

TEST(SimpleTransactions, CanGetReplaceMixedObjectStrings)
{
    auto cluster = ClientTestEnvironment::get_cluster();
    SimpleObject o{ "someone", 100 };
    SimpleObject o2{ "someone else", 200 };
    nlohmann::json j2 = o2;
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    couchbase::transactions::transactions txn(*cluster, cfg);

    // upsert initial doc
    auto coll = cluster->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    ASSERT_TRUE(coll->upsert(id, o).is_success());
    txn.run([&](attempt_context& ctx) {
        auto doc = ctx.get(coll, id);
        ctx.replace(coll, doc, j2.dump());
    });
    ASSERT_EQ(o2, coll->get(id).content_as<SimpleObject>());
}
