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

#include <thread>

#include "transactions_env.h"
#include <couchbase/errors.hxx>
#include <couchbase/transactions.hxx>
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

using namespace couchbase;

static nlohmann::json content = nlohmann::json::parse("{\"some number\": 0}");

TEST(ThreadedTransactions, CanGetReplace)
{
    auto& cluster = TransactionsTestEnvironment::get_cluster();
    transactions::transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    cfg.expiration_time(std::chrono::seconds(10));
    transactions::transactions txn(cluster, cfg);
    std::vector<std::thread> threads;

    // upsert initial doc
    auto id = TransactionsTestEnvironment::get_document_id();
    ASSERT_TRUE(TransactionsTestEnvironment::upsert_doc(id, content.dump()));

    int num_threads = 10;
    int num_iterations = 10;

    // use counter to be sure all txns were successful
    std::atomic<uint64_t> counter{ 0 };
    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&]() {
            EXPECT_NO_THROW({
                try {
                    bool done = false;
                    while (!done) {
                        txn.run([&](transactions::attempt_context& ctx) {
                            auto doc = ctx.get(id);
                            auto content = doc.content<nlohmann::json>();
                            auto count = content["some number"].get<uint64_t>();
                            done = (count >= 100);
                            if (!done) {
                                content["some number"] = count + 1;
                                // keep track of # of attempts
                                counter++;
                                ctx.replace(doc, content);
                            }
                        });
                    }
                } catch (const transactions::transaction_exception& e) {
                    std::cout << "exception!";
                }
            });
        });
    }
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    auto final_content = TransactionsTestEnvironment::get_doc(id).content_as<nlohmann::json>();
    ASSERT_EQ(final_content["some number"].get<uint64_t>(), 100);
    // we increment counter each time through the lambda - chances are high there
    // will be retries, so this will be at least 100
    ASSERT_GE(counter.load(), 100);
}

TEST(ThreadedTransactions, CanInsertThenGetRemove)
{
    auto& c = TransactionsTestEnvironment::get_cluster();
    transactions::transaction_config cfg;
    cfg.cleanup_client_attempts(false);
    cfg.cleanup_lost_attempts(false);
    cfg.expiration_time(std::chrono::seconds(10));
    transactions::transactions txn(c, cfg);
    std::vector<std::thread> threads;

    // More threads than we have bucket instances
    int num_threads = 10;
    int num_iterations = 10;

    // use counter to be sure all txns were successful
    std::atomic<uint64_t> counter{ 0 };
    std::atomic<uint64_t> expired{ 0 };
    std::string id_prefix = couchbase::transactions::uid_generator::next();

    // threadsafe if only stuff on stack
    std::function<couchbase::document_id(int)> get_id = [id_prefix](int i) -> couchbase::document_id {
        std::stringstream stream;
        stream << id_prefix << "_" << i;
        return TransactionsTestEnvironment::get_document_id(stream.str());
    };

    for (int i = 0; i < num_threads; i++) {
        threads.emplace_back([&, i]() {
            EXPECT_NO_THROW({
                auto id = get_id(i);
                for (int j = 0; j < num_iterations; j++) {
                    try {
                        ASSERT_TRUE(TransactionsTestEnvironment::insert_doc(id, content.dump()));
                        txn.run([&](transactions::attempt_context& ctx) {
                            auto doc = ctx.get(id);
                            ctx.remove(doc);
                            ++counter;
                        });
                    } catch (transactions::transaction_expired& e) {
                        ++expired;
                    }
                }
            });
        });
    }

    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    // we are not contending for same doc in this one, so counter should be exact.
    ASSERT_EQ(counter.load() + expired.load(), num_threads * num_iterations);
    for (int i = 0; i < num_threads; i++) {
        couchbase::operations::get_request req{ get_id(i) };
        auto barrier = std::make_shared<std::promise<std::error_code>>();
        auto f = barrier->get_future();
        c.execute(req, [barrier](couchbase::operations::get_response resp) mutable { barrier->set_value(resp.ctx.ec); });
        ASSERT_TRUE(f.get() == couchbase::error::key_value_errc::document_not_found);
    }
}
