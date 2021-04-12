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

#include "client_env.h"
#include <couchbase/client/collection.hxx>
#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

using namespace couchbase;

static nlohmann::json content = nlohmann::json::parse("{\"some number\": 0}");
testing::AssertionResult
no_lost_instances(std::shared_ptr<bucket> b)
{
    if (b->instances() == b->available_instances()) {
        return testing::AssertionSuccess();
    }
    return testing::AssertionFailure() << "bucket " << b->name() << " instances (" << b->instances() << ") not equal available instances ("
                                       << b->available_instances() << ")";
}
testing::AssertionResult
no_lost_instances(cluster& c)
{
    if (c.instances() == c.available_instances()) {
        return testing::AssertionSuccess();
    }
    return testing::AssertionFailure() << "cluster instances (" << c.instances() << ") not equal available instances ("
                                       << c.available_instances() << ")";
}

testing::AssertionResult
all_instances_created(std::shared_ptr<bucket> b)
{
    if (b->instances() == b->max_instances()) {
        return testing::AssertionSuccess();
    }
    return testing::AssertionFailure() << "bucket instances (" << b->instances() << ") not equal max_instances (" << b->max_instances()
                                       << ")";
}
TEST(ThreadedClusterTests, CanSetInstances)
{
    auto c = ClientTestEnvironment::get_cluster();
    cluster c2(c->cluster_address(), "Administrator", "password", cluster_options().max_instances(2).max_bucket_instances(5));
    no_lost_instances(*c);
    no_lost_instances(c2);
    ASSERT_EQ(c2.available_instances(), 2);
    auto b = c2.bucket("default");
    no_lost_instances(b);
    ASSERT_EQ(b->available_instances(), 5);
}

TEST(ThreadedClusterTests, CanOpenMultipleBuckets)
{
    // N threads open buckets, without sadness and tragedy.
    const int NUM_ITERATIONS = 100;
    auto c = ClientTestEnvironment::get_cluster();
    no_lost_instances(*c);
    auto default_bucket = c->bucket("default");
    auto sec_bucket = c->bucket("secBucket");
    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_ITERATIONS; i++) {
        threads.emplace_back([&]() {
            // secBucket expected for fit tests, so expect it here, in addition
            // to default bucket.
            EXPECT_NO_THROW({
                ASSERT_EQ(c->bucket("default"), default_bucket);
                ASSERT_EQ(c->bucket("secBucket"), sec_bucket);
            });
        });
    }
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    no_lost_instances(default_bucket);
    no_lost_instances(sec_bucket);
    no_lost_instances(*c);
}
TEST(ThreadedClusterTests, CanOpenBucketAndUse)
{
    // We already have "default" bucket open.  Lets
    // race opening a different bucket, and be sure things
    // work sensibly.
    const int NUM_ITERATIONS = 10;
    auto c = ClientTestEnvironment::get_cluster();
    no_lost_instances(*c);
    std::vector<std::thread> threads;
    std::atomic<uint64_t> counter{ 0 };
    auto id = ClientTestEnvironment::get_uuid();
    for (int i = 0; i < NUM_ITERATIONS; i++) {
        threads.emplace_back([&]() {
            EXPECT_NO_THROW({
                auto coll = c->bucket("secBucket")->default_collection();
                if (!coll->exists(id).value->get<bool>()) {
                    if (coll->insert(id, content).is_success()) {
                        return;
                    }
                }
                auto get_res = coll->get(id);
                auto new_content = get_res.value->get<nlohmann::json>();
                new_content["another num"] = ++counter;
                auto upsert_res = coll->upsert(id, content);
                ASSERT_TRUE(upsert_res.is_success());
            });
        });
    }
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
}
TEST(ThreadedCollectionTests, CanUseMultipleBuckets)
{
    auto c = ClientTestEnvironment::get_cluster();
    no_lost_instances(*c);
    std::vector<std::thread> threads;
    std::atomic<uint64_t> counter{ 0 };
    auto c1 = c->bucket("default")->default_collection();
    auto c2 = c->bucket("secBucket")->default_collection();
    const int NUM_ITERATIONS = 100;
    const int NUM_THREADS = 2 * c2->get_bucket()->max_instances();
    const auto id = ClientTestEnvironment::get_uuid();
    // Since the first use of the secBucket could be here, we want
    // to extend the kv_timeout.  This really is only an issue for
    // slow clusters (cbdyncluster in Jenkins, for instance).
    auto opts = upsert_options().timeout(std::chrono::seconds(10));
    for (int i = 0; i < NUM_THREADS; i++) {
        threads.emplace_back([&]() {
            EXPECT_NO_THROW({
                for (int j = 0; j < NUM_ITERATIONS; j++) {
                    auto new_content = content;
                    new_content["new thing"] = ++counter;
                    auto res = c1->upsert(id, new_content, opts);
                    auto res2 = c2->upsert(id, new_content, opts);
                    // it is ok to timeout - could be we are starving a thread while some others are
                    // making connections.  Just as long as we eventually succeed, this is fine.
                    if (res2.is_timeout() || res.is_timeout()) {
                        --j;
                        continue;
                    }
                    ASSERT_TRUE(res.is_success()) << "default bucket upsert result was: " << res.rc;
                    ASSERT_TRUE(res2.is_success()) << "secBucket upsert result was: " << res2.rc;
                }
            });
        });
    }
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    ASSERT_EQ(NUM_ITERATIONS * NUM_THREADS, counter.load());
    // NOTE: there is a chance that the last upsert in the last iteration of a couple
    // threads are racing, and so the final value isn't the same as the counter's value.
    // So, GE instead of EQ for that check.
    auto r = c1->get(id);
    ASSERT_GE(counter.load(), r.value->get<nlohmann::json>()["new thing"].get<uint64_t>());
    auto r2 = c2->get(id);
    ASSERT_GE(counter.load(), r2.value->get<nlohmann::json>()["new thing"].get<uint64_t>());
}
TEST(ThreadedCollectionTests, CanGetAndUpsert)
{
    auto coll = ClientTestEnvironment::get_cluster()->bucket("default")->default_collection();
    auto id = ClientTestEnvironment::get_uuid();
    auto result = coll->upsert(id, content);
    const int NUM_ITERATIONS{ 10 };
    const int NUM_THREADS = 2 * coll->get_bucket()->max_instances();
    std::atomic<uint64_t> counter{ 0 };
    ASSERT_TRUE(result.is_success());
    // do simple get+upsert on same doc in multiple threads - make sure there are more
    // threads than max_instance for the bucket.
    std::vector<std::thread> threads;
    for (int i = 0; i < NUM_THREADS; i++) {
        threads.emplace_back([&]() {
            EXPECT_NO_THROW({
                for (int j = 0; j < NUM_ITERATIONS; j++) {
                    auto res = coll->get(id);
                    auto val = res.value->get<nlohmann::json>();
                    val["another_num"] = ++counter;
                    auto res2 = coll->upsert(id, val);
                }
            });
        });
    }
    for (auto& t : threads) {
        if (t.joinable()) {
            t.join();
        }
    }
    // NOTE: there is a chance that the last upsert in the last iteration of a couple
    // threads are racing, and so the final value isn't the same as the counter's value.
    // So, LE instead of EQ for that check.
    ASSERT_EQ(NUM_ITERATIONS * NUM_THREADS, counter.load());
    auto val = coll->get(id).value->get<nlohmann::json>();
    ASSERT_LE(val["another_num"].get<uint64_t>(), counter.load());
    no_lost_instances(*ClientTestEnvironment::get_cluster());
    all_instances_created(coll->get_bucket());
    no_lost_instances(coll->get_bucket());
}
