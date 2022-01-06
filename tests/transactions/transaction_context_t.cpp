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

TEST(SimpleTxnContext, CanDoSimpleTxn)
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
        ASSERT_TRUE(res);
        ASSERT_FALSE(err);
        tx.replace(*res, new_content.dump(), [&](std::exception_ptr err, std::optional<transaction_get_result> replaced) {
            ASSERT_TRUE(replaced);
            ASSERT_FALSE(err);
            tx.commit([&](std::exception_ptr err) {
                ASSERT_FALSE(err);
                txn_completed(err, barrier);
            });
        });
    });
    f.get();
    ASSERT_EQ(TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>(), new_content);
}
