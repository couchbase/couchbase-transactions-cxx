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

#include "../../src/transactions/waitable_op_list.hxx"
#include <future>
#include <gtest/gtest.h>
#include <list>
#include <random>

static const std::string NODE{ "someipaddress" };

TEST(WaitableOpList, DefaultsToKVMode)
{
    couchbase::transactions::waitable_op_list op_list;
    auto mode = op_list.get_mode();
    ASSERT_TRUE(mode.query_node.empty());
    ASSERT_EQ(mode.mode, couchbase::transactions::attempt_mode::modes::KV);
}

TEST(WaitableOpList, CanSetModeAndNode)
{
    couchbase::transactions::waitable_op_list op_list;
    std::atomic<bool> begin_work_called{ false };
    std::atomic<bool> do_work_called{ false };
    op_list.increment_ops();
    op_list.set_query_mode(
      [&op_list, &begin_work_called]() {
          op_list.set_query_node(NODE);
          begin_work_called = true;
      },
      [&do_work_called]() { do_work_called = true; });

    auto mode = op_list.get_mode();
    ASSERT_EQ(mode.query_node, NODE);
    ASSERT_EQ(mode.mode, couchbase::transactions::attempt_mode::modes::QUERY);
    ASSERT_TRUE(begin_work_called.load());
    ASSERT_FALSE(do_work_called.load());
}

TEST(WaitableOpList, SetModeWaitsOnInFlightOps)
{
    couchbase::transactions::waitable_op_list op_list;
    op_list.increment_ops();
    op_list.increment_ops();
    std::atomic<bool> do_work_called{ false };
    auto f = std::async(std::launch::async, [&] {
        op_list.set_query_mode([&]() { op_list.set_query_node(NODE); }, [&do_work_called]() { do_work_called = true; });
    });
    auto f2 = std::async(std::launch::async, [&] {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        op_list.decrement_in_flight();
    });
    ASSERT_EQ(std::future_status::timeout, f.wait_for(std::chrono::milliseconds(100)));
    f2.get();
    ASSERT_EQ(std::future_status::ready, f.wait_for(std::chrono::milliseconds(100)));
    auto mode = op_list.get_mode();
    ASSERT_EQ(mode.mode, couchbase::transactions::attempt_mode::modes::QUERY);
    ASSERT_FALSE(do_work_called.load());
}

TEST(WaitableOpList, SetModeCallsAppropriateCallbacks)
{
    int NUM_FUTURES{ 10 };
    couchbase::transactions::waitable_op_list op_list;
    std::atomic<int> do_work_calls{ 0 };
    std::atomic<int> begin_work_calls{ 0 };
    auto call_set_query_mode = [&]() {
        op_list.increment_ops();
        op_list.set_query_mode(
          [&] {
              begin_work_calls++;
              op_list.set_query_node(NODE);
                  op_list.decrement_in_flight();
                  op_list.decrement_ops();
          },
          [&]() {
              do_work_calls++;
                  op_list.decrement_in_flight();
                  op_list.decrement_ops();
          });
    };

    std::list<std::future<void>> futures;
    for (int i = 0; i < NUM_FUTURES; i++) {
        futures.emplace_back(std::async(std::launch::async, call_set_query_mode));
    }
    for (auto& f : futures) {
        f.get();
    }
    ASSERT_EQ(do_work_calls.load(), NUM_FUTURES - 1);
    ASSERT_EQ(begin_work_calls.load(), 1);
}

TEST(WaitableOpList, GetModeWaits)
{
    couchbase::transactions::waitable_op_list op_list;
    std::atomic<bool> begin_work_called{ false };
    std::atomic<bool> do_work_called{ false };
    op_list.increment_ops();
    op_list.set_query_mode([&begin_work_called]() { begin_work_called = true; }, [&do_work_called]() { do_work_called = true; });
    auto f = std::async(std::launch::async, [&op_list] {
        auto mode = op_list.get_mode();
        ASSERT_EQ(mode.query_node, NODE);
        ASSERT_EQ(mode.mode, couchbase::transactions::attempt_mode::modes::QUERY);
        return;
    });
    auto f2 = std::async(std::launch::async, [&op_list] {
        std::this_thread::sleep_for(std::chrono::seconds(1));
        op_list.set_query_node(NODE);
        return;
    });
    ASSERT_EQ(std::future_status::timeout, f.wait_for(std::chrono::milliseconds(100)));
    f2.get();
    auto mode = op_list.get_mode();
    ASSERT_EQ(mode.query_node, NODE);
    ASSERT_EQ(mode.mode, couchbase::transactions::attempt_mode::modes::QUERY);
}
