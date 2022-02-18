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

void
txn_completed(std::optional<transaction_exception> err,
              std::optional<transaction_result> result,
              std::shared_ptr<std::promise<void>> barrier)
{
    if (err) {
        barrier->set_exception(std::make_exception_ptr(*err));
    } else {
        barrier->set_value();
    }
}

TEST(SimpleAsyncTxns, AsyncGet)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    std::atomic<bool> success = false;
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    txns.run(
      [id, &success](async_attempt_context& ctx) {
          ctx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> res) {
              if (!err) {
                  success = true;
                  ASSERT_TRUE(res);
                  ASSERT_EQ(res->content<nlohmann::json>(), async_content);
              }
          });
      },
      [barrier, &success](std::optional<transaction_exception> err, std::optional<transaction_result> res) {
          txn_completed(std::move(err), res, barrier);
          ASSERT_TRUE(success.load());
      });
    f.get();
}
TEST(SimpleAsyncTxns, CantGetFromUnknownBucket)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    couchbase::document_id bad_id{ "secBucket", "_default", "default", uid_generator::next() };
    std::atomic<bool> cb_called{ false };
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    txns.run(
      [&, barrier](async_attempt_context& ctx) {
          ctx.get(bad_id, [&, barrier](std::exception_ptr err, std::optional<transaction_get_result> result) {
              cb_called = true;
              EXPECT_TRUE(err);
              EXPECT_FALSE(result);
          });
      },
      [barrier, &cb_called](std::optional<transaction_exception> err, std::optional<transaction_result> res) {
          txn_completed(err, res, barrier);
          EXPECT_TRUE(cb_called.load());
      });
    EXPECT_THROW(f.get(), transaction_exception);
    EXPECT_TRUE(cb_called);
}

TEST(SimpleAsyncTxns, AsyncGetFail)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    std::atomic<bool> cb_called = false;
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    try {
        txns.run(
          [&cb_called, id](async_attempt_context& ctx) {
              ctx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> res) {
                  // should be an error
                  ASSERT_TRUE(err);
                  cb_called = true;
              });
          },
          [barrier, &cb_called](std::optional<transaction_exception> err, std::optional<transaction_result> res) {
              txn_completed(std::move(err), res, barrier);
              ASSERT_TRUE(cb_called.load());
          });
        f.get();
        FAIL() << "expected transaction_exception!";
    } catch (const transaction_exception& e) {
        // nothing to do here, but make sure
        ASSERT_TRUE(cb_called.load());
        ASSERT_EQ(e.type(), failure_type::FAIL);
    } catch (const std::exception&) {
        FAIL() << "expected a transaction_failed exception, but got something else";
    }
}

TEST(SimpleAsyncTxns, AsyncRemoveFail)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    std::atomic<bool> cb_called = false;
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    try {
        txns.run(
          [&cb_called, id](async_attempt_context& ctx) {
              ctx.get(id, [&ctx, &cb_called](std::exception_ptr err, std::optional<transaction_get_result> res) {
                  // lets just change the cas to make it fail, which it should
                  // do until timeout
                  if (!err) {
                      res->cas(100);
                      ctx.remove(*res, [&cb_called](std::exception_ptr err) {
                          EXPECT_TRUE(err);
                          cb_called = true;
                      });
                  }
              });
          },
          [barrier, &cb_called](std::optional<transaction_exception> err, std::optional<transaction_result> res) {
              txn_completed(err, res, barrier);
              ASSERT_TRUE(cb_called.load());
          });
        f.get();
        FAIL() << "expected txn to fail until timeout, or error out during rollback";
    } catch (const transaction_exception&) {
        ASSERT_TRUE(cb_called.load());
    }
}
TEST(SimpleAsyncTxns, RYOWOnInsert)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    std::atomic<bool> cb_called = false;
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    txns.run(
      [&cb_called, id](async_attempt_context& ctx) {
          ctx.insert(id, async_content, [&ctx, &cb_called, id](std::exception_ptr err, std::optional<transaction_get_result> res) {
              EXPECT_FALSE(err);
              EXPECT_TRUE(res);
              ctx.get(id, [&ctx, &cb_called, id](std::exception_ptr err, std::optional<transaction_get_result> res) {
                  EXPECT_FALSE(err);
                  EXPECT_TRUE(res);
                  EXPECT_EQ(res->content<nlohmann::json>(), async_content);
                  cb_called = !!res;
              });
          });
      },
      [barrier, &cb_called](std::optional<transaction_exception> err, std::optional<transaction_result> res) {
          txn_completed(err, res, barrier);
          EXPECT_FALSE(err);
          EXPECT_TRUE(res);
          EXPECT_TRUE(cb_called.load());
      });
    f.get();
    EXPECT_TRUE(cb_called.load());
}

TEST(SimpleAsyncTxns, AsyncRemove)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    std::atomic<bool> cb_called = false;
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    txns.run(
      [&cb_called, id](async_attempt_context& ctx) {
          ctx.get(id, [&ctx, &cb_called](std::exception_ptr err, std::optional<transaction_get_result> res) {
              if (!err) {
                  ctx.remove(*res, [&cb_called](std::exception_ptr err) {
                      EXPECT_FALSE(err);
                      cb_called = true;
                  });
              }
          });
      },
      [barrier, &cb_called](std::optional<transaction_exception> err, std::optional<transaction_result> res) {
          txn_completed(std::move(err), res, barrier);
          EXPECT_TRUE(cb_called.load());
      });
    f.get();
    ASSERT_TRUE(cb_called.load());
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
    std::atomic<bool> cb_called = false;
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    txns.run(
      [&cb_called, &new_content, id](async_attempt_context& ctx) {
          ctx.get(id, [&ctx, &new_content, &cb_called](std::exception_ptr err, std::optional<transaction_get_result> res) {
              if (!err) {
                  ctx.replace(*res,
                              new_content,
                              [old_cas = res->cas(), &cb_called](std::exception_ptr err, std::optional<transaction_get_result> result) {
                                  // replace doesn't actually put the new content in the
                                  // result, but it does change the cas, so...
                                  EXPECT_FALSE(err);
                                  EXPECT_NE(result->cas(), old_cas);
                                  cb_called = true;
                              });
              }
          });
      },
      [barrier, &cb_called](std::optional<transaction_exception> err, std::optional<transaction_result> res) {
          txn_completed(std::move(err), res, barrier);
          EXPECT_TRUE(cb_called.load());
      });
    f.get();
    ASSERT_TRUE(cb_called.load());
    auto content = TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>();
    ASSERT_EQ(content, new_content);
}

TEST(SimpleAsyncTxns, AsyncReplaceFail)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto new_content = nlohmann::json::parse("{\"shiny\":\"and new\"}");
    std::atomic<bool> cb_called = false;
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    try {
        txns.run(
          [&cb_called, &new_content, id](async_attempt_context& ctx) {
              ctx.get(id, [&ctx, &new_content, &cb_called](std::exception_ptr err, std::optional<transaction_get_result> res) {
                  if (!err) {
                      ctx.replace(*res, new_content, [&cb_called](std::exception_ptr err, std::optional<transaction_get_result> result) {
                          if (!err) {
                              cb_called = true;
                              throw std::runtime_error("I wanna roll back");
                          }
                      });
                  }
              });
          },
          [barrier, &cb_called](std::optional<transaction_exception> err, std::optional<transaction_result> res) {
              txn_completed(std::move(err), res, barrier);
              EXPECT_TRUE(cb_called.load());
          });
        f.get();
        FAIL() << "expected exception";
    } catch (const transaction_exception& e) {
        ASSERT_TRUE(cb_called.load());
        auto content = TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>();
        ASSERT_EQ(content, async_content);
        ASSERT_EQ(e.type(), failure_type::FAIL);
    };
}

TEST(SimpleAsyncTxns, AsyncInsert)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    std::atomic<bool> cb_called = false;
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    txns.run(
      [&cb_called, id](async_attempt_context& ctx) {
          ctx.insert(id, async_content, [&cb_called](std::exception_ptr err, std::optional<transaction_get_result> res) {
              if (!err) {
                  ASSERT_NE(0, res->cas());
                  cb_called = true;
              }
          });
      },
      [barrier, &cb_called](std::optional<transaction_exception> err, std::optional<transaction_result> res) {
          txn_completed(std::move(err), res, barrier);
          EXPECT_TRUE(cb_called.load());
      });
    f.get();
    ASSERT_TRUE(cb_called.load());
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), async_content);
}

TEST(SimpleAsyncTxns, AsyncInsertFail)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    std::atomic<bool> done = false;
    try {
        txns.run(
          [&done, id, barrier](async_attempt_context& ctx) {
              ctx.insert(id, async_content, [&done](std::exception_ptr err, std::optional<transaction_get_result> res) {
                  if (!err) {
                      done = true;
                      throw std::runtime_error("I wanna rollback");
                  }
              });
          },
          [barrier](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
              txn_completed(err, result, barrier);
              EXPECT_TRUE(err);
              EXPECT_EQ(err->type(), failure_type::FAIL);
          });
        f.get();
        FAIL() << "Expected exception";
    } catch (const transaction_exception& e) {
        ASSERT_TRUE(done.load());
        ASSERT_EQ(e.type(), failure_type::FAIL);
        try {
            TransactionsTestEnvironment::get_doc(id);
            FAIL() << "expected get_doc to raise client exception";
        } catch (const client_error& e) {
            ASSERT_EQ(e.res()->ec, couchbase::error::key_value_errc::document_not_found);
        }
    }
}

TEST(SimpleQueryAsyncTxns, AsyncQuery)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    auto f = barrier->get_future();
    auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
    std::atomic<bool> query_called = false;
    txns.run(
      [&query_called, &query](async_attempt_context& ctx) {
          ctx.query(query, [&](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
              if (!err) {
                  query_called = true;
              }
          });
      },
      [&query_called, barrier](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
          txn_completed(err, result, barrier);
          EXPECT_TRUE(query_called.load());
          EXPECT_FALSE(err);
      });
    f.get();
    ASSERT_TRUE(query_called.load());
    auto content = TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>();
    ASSERT_EQ(content["some"].get<std::string>(), std::string("thing else"));
}

TEST(SimpleQueryAsyncTxns, MultipleRacingQueries)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    auto f = barrier->get_future();
    auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
    std::atomic<int> query_called = 0;
    txns.run(
      [&query_called, &query](async_attempt_context& ctx) {
          ctx.query(query, [&](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
              if (!err) {
                  query_called++;
              }
          });
          ctx.query(query, [&](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
              if (!err) {
                  query_called++;
              }
          });
          ctx.query(query, [&](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
              if (!err) {
                  query_called++;
              }
          });
      },
      [&query_called, barrier](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
          txn_completed(err, result, barrier);
          EXPECT_EQ(3, query_called.load());
          EXPECT_FALSE(err);
      });
    f.get();
    ASSERT_TRUE(query_called.load());
    auto content = TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>();
    ASSERT_EQ(content["some"].get<std::string>(), std::string("thing else"));
}

TEST(SimpleQueryAsyncTxns, RollbackAsyncQuery)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    auto f = barrier->get_future();
    auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
    std::atomic<bool> query_called = false;
    txns.run(
      [&query_called, &query](async_attempt_context& ctx) {
          ctx.query(query, [&](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
              if (!err) {
                  query_called = true;
                  // now rollback by throwing arbitrary exception
                  throw 3;
              }
          });
      },
      [&query_called, barrier](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
          txn_completed(err, result, barrier);
          EXPECT_TRUE(query_called.load());
          EXPECT_TRUE(err);
      });
    EXPECT_THROW({ f.get(); }, transaction_exception);
    ASSERT_TRUE(query_called.load());
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), async_content);
}

TEST(SimpleQueryAsyncTxns, AsyncKVGet)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
    std::atomic<bool> get_called = false;
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    txns.run(
      [&get_called, &query, &id](async_attempt_context& ctx) {
          ctx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
              ctx.query(query, [&](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
                  if (!err) {
                      ctx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
                          if (!err) {
                              EXPECT_EQ(result->content<nlohmann::json>()["some"].get<std::string>(), std::string("thing else"));
                              get_called = true;
                          }
                      });
                  }
              });
          });
      },
      [&get_called, barrier](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
          txn_completed(err, result, barrier);
          EXPECT_TRUE(get_called.load());
          EXPECT_FALSE(err);
      });
    f.get();
    ASSERT_TRUE(get_called.load());
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>()["some"].get<std::string>(), "thing else");
}
TEST(SimpleQueryAsyncTxns, RollbackAsyncKVGet)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    auto query = fmt::format("UPDATE `{}` USE KEYS '{}' SET `some` = 'thing else'", id.bucket(), id.key());
    std::atomic<bool> get_called = false;
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    txns.run(
      [&get_called, &query, &id](async_attempt_context& ctx) {
          ctx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
              ctx.query(query, [&](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
                  if (!err) {
                      ctx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
                          if (!err) {
                              EXPECT_EQ(result->content<nlohmann::json>()["some"].get<std::string>(), std::string("thing else"));
                              get_called = true;
                              throw 3;
                          }
                      });
                  }
              });
          });
      },
      [&get_called, barrier](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
          txn_completed(err, result, barrier);
          EXPECT_TRUE(get_called.load());
          EXPECT_TRUE(err);
      });
    ASSERT_THROW(f.get(), transaction_exception);
    ASSERT_TRUE(get_called.load());
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>()["some"].get<std::string>(), "thing");
}

TEST(SimpleQueryAsyncTxns, AsyncKVInsert)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    std::atomic<bool> insert_called = false;
    txns.run(
      [&, barrier](async_attempt_context& ctx) {
          ctx.query("Select 'Yo' as greeting",
                    [&, barrier](std::exception_ptr err, std::optional<couchbase::operations::query_response> resp) {
                        if (!err) {
                            ctx.insert(id, async_content.dump(), [&](std::exception_ptr err, std::optional<transaction_get_result> res) {
                                insert_called = !err;
                            });
                        }
                    });
      },
      [&, barrier](std::optional<transaction_exception> err, std::optional<transaction_result> res) {
          txn_completed(err, res, barrier);
          EXPECT_FALSE(err);
          EXPECT_TRUE(insert_called.load());
      });
    f.get();
    ASSERT_TRUE(insert_called.load());
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), async_content);
}

TEST(SimpleQueryAsyncTxns, RollbackAsyncKVInsert)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    std::atomic<bool> insert_called = false;
    txns.run(
      [&, barrier](async_attempt_context& ctx) {
          ctx.query("Select 'Yo' as greeting",
                    [&, barrier](std::exception_ptr err, std::optional<couchbase::operations::query_response> resp) {
                        if (!err) {
                            ctx.insert(id, async_content.dump(), [&](std::exception_ptr err, std::optional<transaction_get_result> res) {
                                insert_called = !err;
                                // now roll it back
                                throw 3;
                            });
                        }
                    });
      },
      [&, barrier](std::optional<transaction_exception> err, std::optional<transaction_result> res) {
          txn_completed(err, res, barrier);
          EXPECT_TRUE(err);
          EXPECT_TRUE(insert_called.load());
      });
    ASSERT_THROW(f.get(), transaction_exception);
    ASSERT_TRUE(insert_called.load());
    try {
        TransactionsTestEnvironment::get_doc(id);
    } catch (const client_error& e) {
        ASSERT_EQ(e.res()->ec, couchbase::error::key_value_errc::document_not_found);
    }
}

TEST(SimpleQueryAsyncTxns, AsyncKVReplace)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
    auto new_content = nlohmann::json::parse("{\"some\": \"thing else\"}");
    std::atomic<bool> replace_called = false;
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    txns.run(
      [&replace_called, &query, &id, &new_content](async_attempt_context& ctx) {
          ctx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
              // do a query just to move into query mode.
              if (!err) {
                  EXPECT_TRUE(result);
                  ctx.query(
                    query, [&, doc = *result](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
                        if (!err) {
                            ctx.replace(doc, new_content, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
                                if (!err) {
                                    replace_called = true;
                                }
                            });
                        }
                    });
              }
          });
      },
      [&replace_called, barrier](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
          txn_completed(err, result, barrier);
          EXPECT_TRUE(replace_called.load());
          EXPECT_FALSE(err);
      });
    f.get();
    ASSERT_TRUE(replace_called.load());
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), new_content);
}

TEST(SimpleQueryAsyncTxns, RollbackAsyncKVReplace)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
    auto new_content = nlohmann::json::parse("{\"some\": \"thing else\"}");
    std::atomic<bool> replace_called = false;
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    txns.run(
      [&replace_called, &query, &id, &new_content](async_attempt_context& ctx) {
          ctx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
              // do a query just to move into query mode.
              if (!err) {
                  EXPECT_TRUE(result);
                  ctx.query(
                    query, [&, doc = *result](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
                        if (!err) {
                            ctx.replace(doc, new_content, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
                                if (!err) {
                                    replace_called = true;
                                    throw 3;
                                }
                            });
                        }
                    });
              }
          });
      },
      [&replace_called, barrier](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
          txn_completed(err, result, barrier);
          EXPECT_TRUE(replace_called.load());
          EXPECT_TRUE(err);
      });
    ASSERT_THROW(f.get(), transaction_exception);
    ASSERT_TRUE(replace_called.load());
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), async_content);
}

TEST(SimpleQueryAsyncTxns, AsyncKVRemove)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
    std::atomic<bool> remove_called = false;
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    txns.run(
      [&remove_called, &query, &id](async_attempt_context& ctx) {
          ctx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
              // do a query just to move into query mode.
              if (!err) {
                  EXPECT_TRUE(result);
                  ctx.query(query,
                            [&, doc = *result](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
                                if (!err) {
                                    ctx.remove(doc, [&](std::exception_ptr err) {
                                        if (!err) {
                                            remove_called = true;
                                        }
                                    });
                                }
                            });
              }
          });
      },
      [&remove_called, barrier](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
          txn_completed(err, result, barrier);
          EXPECT_TRUE(remove_called.load());
          EXPECT_FALSE(err);
      });
    f.get();
    ASSERT_TRUE(remove_called.load());
    try {
        TransactionsTestEnvironment::get_doc(id);
    } catch (const client_error& e) {
        ASSERT_EQ(e.res()->ec, couchbase::error::key_value_errc::document_not_found);
    }
}
TEST(SimpleQueryAsyncTxns, RollbackAsyncKVRemove)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto barrier = std::make_shared<std::promise<void>>();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto f = barrier->get_future();
    auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
    std::atomic<bool> remove_called = false;
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, async_content.dump()));
    txns.run(
      [&remove_called, &query, &id](async_attempt_context& ctx) {
          ctx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
              // do a query just to move into query mode.
              if (!err) {
                  EXPECT_TRUE(result);
                  ctx.query(query,
                            [&, doc = *result](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
                                if (!err) {
                                    ctx.remove(doc, [&](std::exception_ptr err) {
                                        ASSERT_FALSE(err);
                                        remove_called = true;
                                        throw 3;
                                    });
                                }
                            });
              }
          });
      },
      [&remove_called, barrier](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
          txn_completed(err, result, barrier);
          EXPECT_TRUE(remove_called.load());
          EXPECT_TRUE(err);
      });
    ASSERT_THROW(f.get(), transaction_exception);
    ASSERT_TRUE(remove_called.load());
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), async_content);
}

TEST(ConcurrentAsyncTxns, AsyncGetReplace)
{
    const size_t NUM_TXNS{ 2 };
    auto doc1_content = nlohmann::json::parse("{\"number\": 0}");
    auto doc2_content = nlohmann::json::parse("{\"number\":200}");
    auto id1 = TransactionsTestEnvironment::get_document_id();
    auto id2 = TransactionsTestEnvironment::get_document_id();
    TransactionsTestEnvironment::upsert_doc(id1, doc1_content.dump());
    TransactionsTestEnvironment::upsert_doc(id2, doc2_content.dump());
    auto txn = TransactionsTestEnvironment::get_transactions();
    std::atomic<uint32_t> attempts{ 0 };
    std::atomic<uint32_t> errors{ 0 };
    std::atomic<uint32_t> txns{ 0 };
    std::atomic<bool> done{ false };
    uint32_t in_flight{ 0 };
    std::condition_variable cv_in_flight;
    std::condition_variable cv_txns_complete;
    std::mutex mut;
    while (!done.load()) {
        std::unique_lock<std::mutex> lock(mut);
        cv_in_flight.wait(lock, [&] { return in_flight < NUM_TXNS; });
        in_flight++;
        lock.unlock();
        txn.run(
          [&](async_attempt_context& ctx) {
              attempts++;
              ctx.get(id1, [&done, &ctx](std::exception_ptr err, std::optional<transaction_get_result> doc1) {
                  if (!doc1 || err) {
                      return;
                  }
                  auto content = doc1->content<nlohmann::json>();
                  auto count = content["number"].get<uint32_t>();
                  if (count >= 200) {
                      done = true;
                      return;
                  }
                  content["number"] = ++count;
                  ctx.replace(*doc1, content, [doc1](std::exception_ptr err, std::optional<transaction_get_result> doc1_updated) {
                      if (!err) {
                          EXPECT_NE(doc1->cas(), doc1_updated->cas());
                      }
                  });
              });
              ctx.get(id2, [&done, &ctx](std::exception_ptr err, std::optional<transaction_get_result> doc2) {
                  if (!doc2 || err) {
                      return;
                  }
                  auto content = doc2->content<nlohmann::json>();
                  auto count = content["number"].get<uint32_t>();
                  if (count <= 0) {
                      done = true;
                      return;
                  }
                  content["number"] = --count;
                  ctx.replace(*doc2, content, [doc2](std::exception_ptr err, std::optional<transaction_get_result> doc2_updated) {
                      if (!err) {
                          EXPECT_NE(doc2->cas(), doc2_updated->cas());
                      }
                  });
              });
          },
          [&](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
              txns++;
              std::unique_lock<std::mutex> lock(mut);
              in_flight--;
              if (in_flight < NUM_TXNS) {
                  cv_in_flight.notify_all();
              }
              if (in_flight == 0 && done.load()) {
                  cv_txns_complete.notify_all();
              }
              lock.unlock();
              if (err) {
                  errors++;
              }
          });
    }

    // wait till it is really done and committed that last one...
    std::unique_lock<std::mutex> lock(mut);
    cv_txns_complete.wait(lock, [&] { return (in_flight == 0 && done.load()); });
    lock.unlock();
    // now lets look at the final state of the docs:
    auto doc1 = TransactionsTestEnvironment::get_doc(id1);
    auto doc2 = TransactionsTestEnvironment::get_doc(id2);
    ASSERT_EQ(0, doc2.content_as<nlohmann::json>()["number"].get<uint32_t>());
    ASSERT_EQ(200, doc1.content_as<nlohmann::json>()["number"].get<uint32_t>());
    // could be we have some txns that are successful, but did nothing as they noticed the count
    // is at limits.  So at least 200 txns.
    ASSERT_GE(txns.load() - errors.load(), 200);
    // No way we don't have at least one conflict, so attempts should be much larger than txns.
    ASSERT_GT(attempts.load(), 200);
    std::cout << "attempts: " << attempts.load() << ", txns: " << txns.load() << ", errors: " << errors.load() << std::endl;
}
