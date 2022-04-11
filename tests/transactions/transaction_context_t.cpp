/*
 *     Copyright 2022 Couchbase, Inc.
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
#include "helpers.hxx"
#include "transactions_env.h"
#include <couchbase/errors.hxx>
#include <couchbase/transactions.hxx>
#include <couchbase/transactions/internal/transaction_context.hxx>
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <future>
#include <stdexcept>

using namespace couchbase::transactions;
auto tx_content = nlohmann::json::parse("{\"some\":\"thing\"}");

void
txn_completed(std::exception_ptr err, std::shared_ptr<std::promise<void>> barrier)
{
    if (err) {
        barrier->set_exception(err);
    } else {
        barrier->set_value();
    }
};

// blocking txn logic wrapper
template<typename Handler>
transaction_result
simple_txn_wrapper(transaction_context& tx, Handler&& handler)
{
    size_t attempts{ 0 };
    while (attempts++ < 1000) {
        auto barrier = std::make_shared<std::promise<std::optional<transaction_result>>>();
        auto f = barrier->get_future();
        tx.new_attempt_context();
        // in transactions.run, we currently handle exceptions that may come back from the
        // txn logic as well (using tx::handle_error).
        handler();
        tx.finalize([barrier](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
            if (err) {
                return barrier->set_exception(std::make_exception_ptr(*err));
            }
            return barrier->set_value(result);
        });
        if (auto res = f.get()) {
            return *res;
        }
    }
    throw std::runtime_error("exceeded max attempts and didn't timeout!");
}

TEST(SimpleTxnContext, CanDoSimpleTxnWithTxWrapper)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    auto new_content = nlohmann::json::parse("{\"some\":\"thing else\"}");

    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, tx_content.dump()));
    transaction_context tx(txns);
    auto txn_logic = [&new_content, &id, &tx]() {
        tx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> res) {
            EXPECT_TRUE(res);
            EXPECT_FALSE(err);
            tx.replace(*res, new_content.dump(), [&](std::exception_ptr err, std::optional<transaction_get_result> replaced) {
                EXPECT_TRUE(replaced);
                EXPECT_FALSE(err);
            });
        });
    };
    auto result = simple_txn_wrapper(tx, txn_logic);
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), new_content);
}

TEST(SimpleTxnContext, CanDoSimpleTxnWithFinalize)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, tx_content.dump()));
    transaction_context tx(txns);
    tx.new_attempt_context();
    auto new_content = nlohmann::json::parse("{\"some\":\"thing else\"}");
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> res) {
        EXPECT_TRUE(res);
        EXPECT_FALSE(err);
        tx.replace(*res, new_content.dump(), [&](std::exception_ptr err, std::optional<transaction_get_result> replaced) {
            EXPECT_TRUE(replaced);
            EXPECT_FALSE(err);
        });
    });
    tx.finalize([&](std::optional<transaction_exception> err, std::optional<transaction_result> result) {
        if (err) {
            return barrier->set_exception(std::make_exception_ptr(*err));
        }
        return barrier->set_value();
    });
    f.get();
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), new_content);
}

TEST(SimpleTxnContext, CanDoSimpleTxnExplicitCommit)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, tx_content.dump()));
    transaction_context tx(txns);
    tx.new_attempt_context();
    auto new_content = nlohmann::json::parse("{\"some\":\"thing else\"}");
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> res) {
        EXPECT_TRUE(res);
        EXPECT_FALSE(err);
        tx.replace(*res, new_content.dump(), [&](std::exception_ptr err, std::optional<transaction_get_result> replaced) {
            EXPECT_TRUE(replaced);
            EXPECT_FALSE(err);
            tx.commit([&](std::exception_ptr err) {
                EXPECT_FALSE(err);
                txn_completed(err, barrier);
            });
        });
    });
    f.get();
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), new_content);
}

TEST(SimpleTxnContext, CanRollbackSimpleTxn)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, tx_content.dump()));
    transaction_context tx(txns);
    tx.new_attempt_context();
    auto new_content = nlohmann::json::parse("{\"some\":\"thing else\"}");
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> res) {
        EXPECT_TRUE(res);
        EXPECT_FALSE(err);
        tx.replace(*res, new_content.dump(), [&](std::exception_ptr err, std::optional<transaction_get_result> replaced) {
            EXPECT_TRUE(replaced);
            EXPECT_FALSE(err);
            // now rollback
            tx.rollback([&](std::exception_ptr err) {
                EXPECT_FALSE(err); // no error rolling back
                barrier->set_value();
            });
        });
    });
    f.get();
    // this should not throw, as errors should be empty.
    tx.existing_error();
}

TEST(SimpleTxnContext, CanGetInsertErrors)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, tx_content.dump()));
    transaction_context tx(txns);
    tx.new_attempt_context();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.insert(id, tx_content.dump(), [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
        // this should result in a transaction_operation_failed exception since it already exists, so lets check it
        EXPECT_TRUE(err);
        EXPECT_FALSE(result);
        if (err) {
            barrier->set_exception(err);
        } else {
            barrier->set_value();
        }
    });
    EXPECT_THROW(f.get(), transaction_operation_failed);
    EXPECT_THROW(tx.existing_error(), transaction_operation_failed);
}

TEST(SimpleTxnContext, CanGetRemoveErrors)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, tx_content.dump()));
    transaction_context tx(txns);
    tx.new_attempt_context();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
        // this should result in a transaction_operation_failed exception since it already exists, so lets check it
        EXPECT_FALSE(err);
        EXPECT_TRUE(result);
        // make a cas mismatch error
        result->cas(100);
        tx.remove(*result, [&](std::exception_ptr err) {
            EXPECT_TRUE(err);
            if (err) {
                barrier->set_exception(err);
            } else {
                barrier->set_value();
            }
        });
    });
    EXPECT_THROW(f.get(), transaction_operation_failed);
    EXPECT_THROW(tx.existing_error(), transaction_operation_failed);
}

TEST(SimpleTxnContext, CanGetReplaceErrors)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, tx_content.dump()));
    transaction_context tx(txns);
    tx.new_attempt_context();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
        // this should result in a transaction_operation_failed exception since it already exists, so lets check it
        EXPECT_FALSE(err);
        EXPECT_TRUE(result);
        // make a cas mismatch error
        result->cas(100);
        tx.replace(*result, tx_content.dump(), [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
            EXPECT_TRUE(err);
            EXPECT_FALSE(result);
            if (err) {
                barrier->set_exception(err);
            } else {
                barrier->set_value();
            }
        });
    });
    EXPECT_THROW(f.get(), transaction_operation_failed);
    EXPECT_THROW(tx.existing_error(), transaction_operation_failed);
}
TEST(SimpleTxnContext, RYOWGetAfterInsert)
{
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    transaction_context tx(txns);
    tx.new_attempt_context();
    auto logic = [&]() {
        tx.insert(id, tx_content.dump(), [&](std::exception_ptr err, std::optional<transaction_get_result> res) {
            EXPECT_FALSE(err);
            EXPECT_TRUE(res);
            tx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> res) {
                EXPECT_FALSE(err);
                EXPECT_EQ(res->content<nlohmann::json>(), tx_content);
            });
        });
    };
    ASSERT_NO_THROW(simple_txn_wrapper(tx, logic));
    ASSERT_NO_THROW(tx.existing_error());
}

TEST(SimpleTxnContext, CanGetGetErrors)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    transaction_context tx(txns);
    tx.new_attempt_context();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    tx.get(id, [&](std::exception_ptr err, std::optional<transaction_get_result> result) {
        // this should result in a transaction_operation_failed exception since it already exists, so lets check it
        EXPECT_TRUE(err);
        EXPECT_FALSE(result);
        if (err) {
            barrier->set_exception(err);
        } else {
            barrier->set_value();
        }
    });
    EXPECT_THROW(f.get(), transaction_operation_failed);
    EXPECT_THROW(tx.existing_error(), transaction_operation_failed);
}

TEST(SimpleTxnContext, CanDoQuery)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    transaction_context tx(txns);
    tx.new_attempt_context();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, tx_content.dump()));
    auto query = fmt::format("SELECT * FROM `{}` USE KEYS '{}'", id.bucket(), id.key());
    transaction_query_options opts;
    tx.query(query, opts, [&](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
        // this should result in a transaction_operation_failed exception since the doc isn't there
        EXPECT_TRUE(payload);
        EXPECT_FALSE(err);
        if (err) {
            barrier->set_exception(err);
        } else {
            barrier->set_value();
        }
    });
    ASSERT_NO_THROW(f.get());
    ASSERT_NO_THROW(tx.existing_error());
}

TEST(SimpleTxnContext, CanSeeSomeQueryErrorsButNoTxnFailed)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();

    transaction_context tx(txns);
    tx.new_attempt_context();
    auto barrier = std::make_shared<std::promise<void>>();
    auto f = barrier->get_future();
    transaction_query_options opts;
    tx.query("jkjkjl;kjlk;  jfjjffjfj", opts, [&](std::exception_ptr err, std::optional<couchbase::operations::query_response> payload) {
        // this should result in a query_exception since the query isn't parseable.
        EXPECT_TRUE(err);
        EXPECT_FALSE(payload);
        if (err) {
            barrier->set_exception(err);
        } else {
            barrier->set_value();
        }
    });
    try {
        f.get();
        FAIL() << "expected future to throw exception";
    } catch (const query_exception& e) {

    } catch (...) {
        auto e = std::current_exception();
        std::cout << "got " << typeid(e).name() << std::endl;
        FAIL() << "expected query_exception to be thrown from the future";
    }
    EXPECT_NO_THROW(tx.existing_error());
}

TEST(SimpleTxnContext, CanSetPerTxnConfig)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    per_transaction_config per_txn_cfg;
    per_txn_cfg.scan_consistency(couchbase::query_scan_consistency::not_bounded);
    per_txn_cfg.expiration_time(std::chrono::milliseconds(1)).kv_timeout(std::chrono::milliseconds(2));
    per_txn_cfg.durability_level(couchbase::transactions::durability_level::NONE);
    transaction_context tx(txns, per_txn_cfg);
    ASSERT_EQ(tx.config().durability_level(), per_txn_cfg.durability_level());
    ASSERT_EQ(tx.config().kv_timeout(), per_txn_cfg.kv_timeout());
    ASSERT_EQ(tx.config().expiration_time(), per_txn_cfg.expiration_time());
    ASSERT_EQ(tx.config().scan_consistency(), per_txn_cfg.scan_consistency());
}
TEST(SimpleTxnContext, CanNotPerTxnConfig)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    auto txns = TransactionsTestEnvironment::get_transactions();
    auto id = TransactionsTestEnvironment::get_document_id();
    transaction_context tx(txns);
    ASSERT_EQ(tx.config().durability_level(), txns.config().durability_level());
    ASSERT_EQ(tx.config().kv_timeout(), txns.config().kv_timeout());
    ASSERT_EQ(tx.config().expiration_time(), txns.config().expiration_time());
    ASSERT_EQ(tx.config().scan_consistency(), txns.config().scan_consistency());
}
