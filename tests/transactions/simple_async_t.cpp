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

#include "../../src/transactions/attempt_context_impl.hxx"
#include "../../src/transactions/attempt_context_testing_hooks.hxx"
#include "helpers.hxx"
#include "transactions_env.h"
#include <couchbase/errors.hxx>
#include <couchbase/transactions.hxx>
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <future>
#include <list>
#include <stdexcept>

using namespace couchbase::transactions;

auto async_content = nlohmann::json::parse("{\"some\": \"thing\"}");

TEST(SimpleAsyncTxns, AsyncGet)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    std::atomic<bool> done = false;
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    auto result = txns.run([id, &done](async_attempt_context& ctx) {
        ctx.get(id, [&](std::optional<transaction_operation_failed> err, std::optional<transaction_get_result> res) {
            if (!err) {
                done = true;
            }
            ASSERT_TRUE(res);
            ASSERT_EQ(res->content<nlohmann::json>(), async_content);
        });
    });
    ASSERT_TRUE(done.load());
}
TEST(SimpleAsyncTxns, AsyncGetFail)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    std::atomic<bool> cb_called = false;
    try {
        txns.run([&cb_called, id](async_attempt_context& ctx) {
            ctx.get(id, [&](std::optional<transaction_operation_failed> err, std::optional<transaction_get_result> res) {
                // should be an error
                ASSERT_TRUE(err);
                cb_called = true;
            });
        });
        FAIL() << "expected transaction_failed!";
    } catch (const transaction_failed&) {
        // nothing to do here, but make sure
        ASSERT_TRUE(cb_called.load());
    }
}

TEST(SimpleAsyncTxns, AsyncRemoveFail)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    std::atomic<bool> done = false;
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    try {
        txns.run([&done, id](async_attempt_context& ctx) {
            ctx.get(id, [&ctx, &done](std::optional<transaction_operation_failed> err, std::optional<transaction_get_result> res) {
                // lets just change the cas to make it fail, which it should
                // do until timeout
                if (!err) {
                    res->cas(100);
                    ctx.remove(*res, [&done](std::optional<transaction_operation_failed> err) {
                        ASSERT_TRUE(err);
                        done = true;
                    });
                }
            });
        });
        FAIL() << "expected txn to fail until timeout";
    } catch (const transaction_expired&) {
        ASSERT_TRUE(done.load());
    }
}

TEST(SimpleAsyncTxns, AsyncRemove)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    std::atomic<bool> done = false;
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    txns.run([&done, id](async_attempt_context& ctx) {
        ctx.get(id, [&ctx, &done](std::optional<transaction_operation_failed> err, std::optional<transaction_get_result> res) {
            ASSERT_FALSE(err);
            ctx.remove(*res, [&done](std::optional<transaction_operation_failed> err) {
                ASSERT_FALSE(err);
                done = true;
            });
        });
    });
    ASSERT_TRUE(done.load());
    try {
        TransactionsTestEnvironment::get_doc(id);
        FAIL() << "expected get_doc to raise client exception";
    } catch (const client_error& e) {
        ASSERT_EQ(e.res()->ec, couchbase::error::key_value_errc::document_not_found);
    }
}
TEST(SimpleAsyncTxns, AsyncReplace)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto new_content = nlohmann::json::parse("{\"shiny\":\"and new\"}");
    std::atomic<bool> done = false;
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    txns.run([&done, &new_content, id](async_attempt_context& ctx) {
        ctx.get(id,
                [&ctx, &new_content, &done](std::optional<transaction_operation_failed> err, std::optional<transaction_get_result> res) {
                    ASSERT_FALSE(err);
                    ctx.replace(*res,
                                new_content,
                                [old_cas = res->cas(), &done](std::optional<transaction_operation_failed> err,
                                                              std::optional<transaction_get_result> result) {
                                    // replace doesn't actually put the new content in the
                                    // result, but it does change the cas, so...
                                    ASSERT_FALSE(err);
                                    ASSERT_NE(result->cas(), old_cas);
                                    done = true;
                                });
                });
    });
    ASSERT_TRUE(done.load());
    auto content = TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>();
    ASSERT_EQ(content, new_content);
}

TEST(SimpleAsyncTxns, AsyncReplaceFail)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto new_content = nlohmann::json::parse("{\"shiny\":\"and new\"}");
    std::atomic<bool> done = false;
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    try {
        txns.run([&done, &new_content, id](async_attempt_context& ctx) {
            ctx.get(
              id, [&ctx, &new_content, &done](std::optional<transaction_operation_failed> err, std::optional<transaction_get_result> res) {
                  ASSERT_FALSE(err);
                  ctx.replace(*res,
                              new_content,
                              [&done](std::optional<transaction_operation_failed> err, std::optional<transaction_get_result> result) {
                                  ASSERT_FALSE(err);
                                  done = true;
                                  throw std::runtime_error("I wanna roll back");
                              });
              });
        });
        FAIL() << "expected exception";
    } catch (const transaction_failed&) {
        ASSERT_TRUE(done.load());
        auto content = TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>();
        ASSERT_EQ(content, async_content);
    };
}
TEST(SimpleAsyncTxns, AsyncInsert)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    std::atomic<bool> done = false;
    txns.run([&done, id](async_attempt_context& ctx) {
        ctx.insert(id, async_content, [&done](std::optional<transaction_operation_failed> err, std::optional<transaction_get_result> res) {
            ASSERT_FALSE(err);
            ASSERT_NE(0, res->cas());
            done = true;
        });
    });
    ASSERT_TRUE(done.load());
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), async_content);
}
TEST(SimpleAsyncTxns, AsyncInsertFail)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    std::atomic<bool> done = false;
    try {
        txns.run([&done, id](async_attempt_context& ctx) {
            ctx.insert(
              id, async_content, [&done](std::optional<transaction_operation_failed> err, std::optional<transaction_get_result> res) {
                  ASSERT_FALSE(err);
                  done = true;
                  throw std::runtime_error("I wanna rollback");
              });
        });
        FAIL() << "Expected exception";
    } catch (const transaction_failed&) {
        ASSERT_TRUE(done.load());
        try {
            TransactionsTestEnvironment::get_doc(id);
            FAIL() << "expected get_doc to raise client exception";
        } catch (const client_error& e) {
            ASSERT_EQ(e.res()->ec, couchbase::error::key_value_errc::document_not_found);
        }
    }
}
TEST(ThreadedAsyncTxns, AsyncGetReplace)
{
    const size_t NUM_TXNS{ 5 };
    auto doc1_content = nlohmann::json::parse("{\"number\": 0}");
    auto doc2_content = nlohmann::json::parse("{\"number\":200}");
    auto id1 = TransactionsTestEnvironment::get_document_id();
    auto id2 = TransactionsTestEnvironment::get_document_id();
    TransactionsTestEnvironment::upsert_doc(id1, doc1_content.dump());
    TransactionsTestEnvironment::upsert_doc(id2, doc2_content.dump());
    auto txn = TransactionsTestEnvironment::get_transactions();
    std::list<std::future<void>> txn_futures;
    std::atomic<uint32_t> attempts{ 0 };
    std::atomic<uint32_t> errors{ 0 };
    std::atomic<bool> done = false;
    txn_futures.emplace_back(std::async(std::launch::async, [&] {
        while (!done.load()) {
            try {
                txn.run([&](async_attempt_context& ctx) {
                    attempts++;
                    auto b1 = std::make_shared<std::promise<void>>();
                    auto b2 = std::make_shared<std::promise<void>>();
                    ctx.get(id1,
                            [&done, &ctx, b1](std::optional<transaction_operation_failed> err, std::optional<transaction_get_result> doc1) {
                                auto content = doc1->content<nlohmann::json>();
                                auto count = content["number"].get<uint32_t>();
                                if (count >= 200) {
                                    done = true;
                                    b1->set_value();
                                    return;
                                }
                                content["number"] = ++count;
                                ctx.replace(*doc1,
                                            content,
                                            [doc1, b1](std::optional<transaction_operation_failed> err,
                                                       std::optional<transaction_get_result> doc1_updated) {
                                                ASSERT_NE(doc1->cas(), doc1_updated->cas());
                                                b1->set_value();
                                            });
                            });
                    ctx.get(id2,
                            [&done, b2, &ctx](std::optional<transaction_operation_failed> err, std::optional<transaction_get_result> doc2) {
                                auto content = doc2->content<nlohmann::json>();
                                auto count = content["number"].get<uint32_t>();
                                if (count <= 0) {
                                    done = true;
                                    b2->set_value();
                                    return;
                                }
                                content["number"] = --count;
                                ctx.replace(*doc2,
                                            content,
                                            [doc2, b2](std::optional<transaction_operation_failed> err,
                                                       std::optional<transaction_get_result> doc2_updated) {
                                                ASSERT_NE(doc2->cas(), doc2_updated->cas());
                                                b2->set_value();
                                            });
                            });
                    // wait on the barriers before commit.
                    b1->get_future().get();
                    b2->get_future().get();
                });
            } catch (const transaction_exception& e) {
                txn_log->trace("got transaction exception {}", e.what());
                errors++;
            }
        }
    }));
    for (auto& f : txn_futures) {
        f.get();
    }
    // now lets look at the final state of the docs:
    auto doc1 = TransactionsTestEnvironment::get_doc(id1);
    auto doc2 = TransactionsTestEnvironment::get_doc(id2);
    ASSERT_EQ(0, doc2.content_as<nlohmann::json>()["number"].get<uint32_t>());
    ASSERT_EQ(200, doc1.content_as<nlohmann::json>()["number"].get<uint32_t>());
}
