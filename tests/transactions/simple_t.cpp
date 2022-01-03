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

#include "helpers.hxx"
#include "transactions_env.h"
#include <couchbase/errors.hxx>
#include <couchbase/transactions.hxx>
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>
#include <stdexcept>

using namespace couchbase::transactions;

auto content = nlohmann::json::parse("{\"some\": \"thing\"}");

TEST(SimpleTransactions, ArbitraryRuntimeError)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    couchbase::transactions::transactions txn(cluster, cfg);
    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, content.dump()));
    EXPECT_THROW(
      {
          try {
              txn.run([&](attempt_context& ctx) {
                  ctx.get(id);
                  throw std::runtime_error("Yo");
              });
          } catch (const transaction_exception& e) {
              EXPECT_EQ(e.cause(), external_exception::UNKNOWN);
              EXPECT_EQ(e.type(), failure_type::FAIL);
              EXPECT_STREQ("Yo", e.what());
              throw;
          }
      },
      transaction_exception);
}

TEST(SimpleTransactions, ArbitraryException)
{
    auto& c = TransactionsTestEnvironment::get_cluster();
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    ::couchbase::transactions::transactions txn(c, cfg);
    auto id = TransactionsTestEnvironment::get_document_id();
    EXPECT_THROW(
      {
          try {
              txn.run([&](attempt_context& ctx) {
                  ctx.insert(id, content);
                  throw 3;
              });
          } catch (const transaction_exception& e) {
              EXPECT_EQ(e.cause(), external_exception::UNKNOWN);
              EXPECT_STREQ("Unexpected error", e.what());
              EXPECT_EQ(e.type(), failure_type::FAIL);
              throw;
          }
      },
      transaction_exception);
}

TEST(SimpleTransactions, CanGetReplace)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    couchbase::transactions::transactions txn(cluster, cfg);

    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, c.dump()));
    txn.run([&](attempt_context& ctx) {
        auto doc = ctx.get(id);
        auto content = doc.content<nlohmann::json>();
        content["another one"] = 1;
        ctx.replace(doc, content);
    });
    // now add to the original content, and compare
    c["another one"] = 1;
    ASSERT_EQ(c, TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>());
}

TEST(SimpleTransactions, CanGetReplaceRawStrings)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto c = nlohmann::json::parse("{\"some number\": 0}");
    std::string new_content("{\"aaa\":\"bbb\"}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    couchbase::transactions::transactions txn(cluster, cfg);

    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, c.dump()));
    txn.run([&](attempt_context& ctx) {
        auto doc = ctx.get(id);
        ctx.replace(doc, new_content);
    });
    ASSERT_EQ(new_content, TransactionsTestEnvironment::get_doc(id).content_as<std::string>());
}

TEST(SimpleTransactions, CanGetReplaceObjects)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    SimpleObject o{ "someone", 100 };
    SimpleObject o2{ "someone else", 200 };
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    couchbase::transactions::transactions txn(cluster, cfg);

    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    nlohmann::json j;
    to_json(j, o);
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, j.dump()));
    txn.run([&](attempt_context& ctx) {
        auto doc = ctx.get(id);
        ctx.replace(doc, o2);
    });
    ASSERT_EQ(o2, TransactionsTestEnvironment::get_doc(id).content_as<SimpleObject>());
}

TEST(SimpleTransactions, CanGetReplaceMixedObjectStrings)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    SimpleObject o{ "someone", 100 };
    SimpleObject o2{ "someone else", 200 };
    nlohmann::json j;
    to_json(j, o);
    nlohmann::json j2 = o2;
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    couchbase::transactions::transactions txn(cluster, cfg);

    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, j.dump()));
    txn.run([&](attempt_context& ctx) {
        auto doc = ctx.get(id);
        ctx.replace(doc, j2.dump());
    });
    ASSERT_EQ(o2, TransactionsTestEnvironment::get_doc(id).content_as<SimpleObject>());
}

TEST(SimpleTransactions, CanRollbackInsert)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    SimpleObject o{ "someone", 100 };
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    couchbase::transactions::transactions txn(cluster, cfg);

    auto id = TransactionsTestEnvironment::get_document_id();
    EXPECT_THROW(
      {
          txn.run([&](attempt_context& ctx) {
              ctx.insert(id, o);
              throw 3; // some arbitrary exception...
          });
      },
      transaction_exception);
    try {
        auto res = TransactionsTestEnvironment::get_doc(id);
        FAIL() << "expect a client_error with document_not_found, got result instead";

    } catch (const client_error& e) {
        ASSERT_EQ(e.res()->ec, couchbase::error::key_value_errc::document_not_found);
    }
}

TEST(SimpleTransactions, CanRollbackRemove)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    couchbase::transactions::transactions txn(cluster, cfg);

    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, c.dump()));
    EXPECT_THROW(
      {
          txn.run([&](attempt_context& ctx) {
              auto res = ctx.get(id);
              auto new_content = nlohmann::json::parse("{\"some number\": 100}");
              ctx.remove(res);
              throw 3; // just throw some arbitrary exception to get rollback
          });
      },
      transaction_exception);
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), c);
}

TEST(SimpleTransactions, CanRollbackReplace)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    couchbase::transactions::transactions txn(cluster, cfg);

    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, c.dump()));
    EXPECT_THROW(
      {
          txn.run([&](attempt_context& ctx) {
              auto res = ctx.get(id);
              auto new_content = nlohmann::json::parse("{\"some number\": 100}");
              ctx.replace(res, new_content);
              throw 3; // just throw some arbitrary exception to get rollback
          });
      },
      transaction_exception);
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), c);
}

TEST(SimpleQueryTransactions, CanHaveTrivialQueryInTxn)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some_number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);

    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, c.dump()));

    std::ostringstream stream;
    stream << "SELECT * FROM `default` USE KEYS '" << id.key() << "'";
    couchbase::transactions::transactions txn(cluster, cfg);
    txn.run([&](attempt_context& ctx) {
        auto payload = ctx.query(stream.str());
        ASSERT_EQ(payload.rows.size(), 1);
        ASSERT_EQ(nlohmann::json::parse(payload.rows.front())["default"], c);
    });
}

TEST(SimpleQueryTransactions, CanModifyDocInQuery)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some_number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);

    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, c.dump()));

    std::ostringstream stream;
    stream << "UPDATE `default` USE KEYS '" << id.key() << "' SET `some_number` = 10";

    couchbase::transactions::transactions txn(cluster, cfg);
    txn.run([&](attempt_context& ctx) {
        auto payload = ctx.query(stream.str());
        ASSERT_FALSE(payload.meta_data.errors);
    });

    auto res = TransactionsTestEnvironment::get_doc(id);
    auto j = res.content_as<nlohmann::json>();
    ASSERT_EQ(10, j["some_number"].get<int>());
}

TEST(SimpleQueryTransactions, CanRollback)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some_number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);

    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, c.dump()));

    std::ostringstream stream;
    stream << "UPDATE `default` USE KEYS '" << id.key() << "' SET `some_number` = 10";

    couchbase::transactions::transactions txn(cluster, cfg);
    EXPECT_THROW(
      {
          txn.run([&](attempt_context& ctx) {
              auto payload = ctx.query(stream.str());
              ASSERT_FALSE(payload.meta_data.errors);
              // now, lets force a rollback
              throw 3;
          });
      },
      transaction_exception);

    auto res = TransactionsTestEnvironment::get_doc(id);
    auto j = res.content_as<nlohmann::json>();
    ASSERT_EQ(j, c);
}

TEST(SimpleQueryTransactions, QueryUpdatesInsert)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some_number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);

    auto id = TransactionsTestEnvironment::get_document_id();

    std::ostringstream stream;
    stream << "UPDATE `default` USE KEYS '" << id.key() << "' SET `some_number` = 10";
    couchbase::transactions::transactions txn(cluster, cfg);
    txn.run([&](attempt_context& ctx) {
        ctx.insert(id, c);
        auto payload = ctx.query(stream.str());
        ASSERT_FALSE(payload.meta_data.errors);
    });

    auto res = TransactionsTestEnvironment::get_doc(id);
    ASSERT_EQ(10, res.content_as<nlohmann::json>()["some_number"].get<int>());
}

TEST(SimpleQueryTransactions, CanKVGet)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some_number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);

    auto id = TransactionsTestEnvironment::get_document_id();

    std::ostringstream stream;
    stream << "UPDATE `default` USE KEYS '" << id.key() << "' SET `some_number` = 10";
    // stream << "SELECT * FROM `default` USE KEYS '" << id.key() << "'";
    couchbase::transactions::transactions txn(cluster, cfg);
    txn.run([&](attempt_context& ctx) {
        ctx.insert(id, c);
        auto payload = ctx.query(stream.str());
        ASSERT_TRUE(payload.rows.empty());
        ASSERT_FALSE(payload.meta_data.errors);
        auto doc = ctx.get(id);
        // TODO: serialize txnMeta once I get it
        // ASSERT_TRUE(doc.links().is_document_in_transaction());
    });
    auto res = TransactionsTestEnvironment::get_doc(id);
    ASSERT_EQ(res.content_as<nlohmann::json>()["some_number"], 10);
}

TEST(SimpleQueryTransactions, CanKVInsert)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some_number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);

    auto id = TransactionsTestEnvironment::get_document_id();
    std::ostringstream stream;
    stream << "SELECT * FROM `default` USE KEYS '" << id.key() << "'";
    couchbase::transactions::transactions txn(cluster, cfg);
    txn.run([&](attempt_context& ctx) {
        auto payload = ctx.query(stream.str());
        ctx.insert(id, c);
    });
    ASSERT_EQ(c, TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>());
}
TEST(SimpleQueryTransactions, CanRollbackKVInsert)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some_number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);

    auto id = TransactionsTestEnvironment::get_document_id();
    std::ostringstream stream;
    stream << "SELECT * FROM `default` USE KEYS '" << id.key() << "'";
    couchbase::transactions::transactions txn(cluster, cfg);
    ASSERT_THROW(
      {
          txn.run([&](attempt_context& ctx) {
              auto payload = ctx.query(stream.str());
              ctx.insert(id, c);
              throw 3;
          });
      },
      transaction_exception);
    try {
        auto doc = TransactionsTestEnvironment::get_doc(id);
        FAIL() << "expected doc to not exist";
    } catch (const client_error& e) {
        ASSERT_EQ(e.res()->ec, couchbase::error::key_value_errc::document_not_found);
    }
}

TEST(SimpleQueryTransactions, CanKVReplace)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some_number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);

    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, c.dump()));

    std::ostringstream stream;
    stream << "SELECT * FROM `default` USE KEYS '" << id.key() << "'";
    couchbase::transactions::transactions txn(cluster, cfg);
    txn.run([&](attempt_context& ctx) {
        auto payload = ctx.query(stream.str());
        auto doc = ctx.get(id);
        auto new_content = doc.content<nlohmann::json>();
        new_content["some_number"] = 10;
        auto replaced_doc = ctx.replace(doc, new_content);
        ASSERT_NE(replaced_doc.cas(), doc.cas());
        ASSERT_NE(0, replaced_doc.cas());
    });
    ASSERT_EQ(10, TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>()["some_number"].get<int>());
}

TEST(SimpleQueryTransactions, CanRollbackKVReplace)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some_number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);

    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, c.dump()));

    std::ostringstream stream;
    stream << "SELECT * FROM `default` USE KEYS '" << id.key() << "'";
    couchbase::transactions::transactions txn(cluster, cfg);
    EXPECT_THROW(
      {
          txn.run([&](attempt_context& ctx) {
              auto payload = ctx.query(stream.str());
              auto doc = ctx.get(id);
              auto new_content = doc.content<nlohmann::json>();
              new_content["some_number"] = 10;
              auto replaced_doc = ctx.replace(doc, new_content);
              ASSERT_NE(replaced_doc.cas(), doc.cas());
              ASSERT_NE(0, replaced_doc.cas());
              throw 3;
          });
      },
      transaction_exception);
    ASSERT_EQ(0, TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>()["some_number"].get<int>());
}

TEST(SimpleQueryTransactions, CanKVRemove)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some_number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);

    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, c.dump()));

    std::ostringstream stream;
    stream << "SELECT * FROM `default` USE KEYS '" << id.key() << "'";
    couchbase::transactions::transactions txn(cluster, cfg);
    txn.run([&](attempt_context& ctx) {
        auto payload = ctx.query(stream.str());
        auto doc = ctx.get(id);
        ctx.remove(doc);
    });
    try {
        auto doc = TransactionsTestEnvironment::get_doc(id);
        FAIL() << "expected doc to not exist";
    } catch (const client_error& e) {
        ASSERT_EQ(e.res()->ec, couchbase::error::key_value_errc::document_not_found);
    }
}

TEST(SimpleQueryTransactions, CanRollbackKVRemove)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    nlohmann::json c = nlohmann::json::parse("{\"some_number\": 0}");
    transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);

    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, c.dump()));

    std::ostringstream stream;
    stream << "SELECT * FROM `default` USE KEYS '" << id.key() << "'";
    couchbase::transactions::transactions txn(cluster, cfg);
    EXPECT_THROW(
      {
          txn.run([&](attempt_context& ctx) {
              auto payload = ctx.query(stream.str());
              auto doc = ctx.get(id);
              ctx.remove(doc);
              throw 3;
          });
      },
      transaction_exception);
    ASSERT_EQ(c, TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>());
}

int
main(int argc, char* argv[])
{
    testing::InitGoogleTest(&argc, argv);
    testing::AddGlobalTestEnvironment(new TransactionsTestEnvironment());
    spdlog::set_level(spdlog::level::trace);
    return RUN_ALL_TESTS();
}
