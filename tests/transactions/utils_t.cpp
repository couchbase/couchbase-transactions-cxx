#include <gtest/gtest.h>

#include <iostream>
#include <limits>
#include <thread>

#include "../../src/transactions/exceptions_internal.hxx"
#include "../../src/transactions/utils.hxx"

using namespace couchbase::transactions;
using namespace std;

double min_jitter_fraction = 1.0 - RETRY_OP_JITTER;

::testing::AssertionResult
IsBetweenInclusive(int val, int a, int b)
{
    if ((val >= a) && (val <= b))
        return ::testing::AssertionSuccess();
    else
        return ::testing::AssertionFailure() << val << " is outside the range " << a << " to " << b;
}

struct retry_state {
    vector<chrono::steady_clock::time_point> timings;

    void reset()
    {
        timings.clear();
    }
    void function()
    {
        timings.push_back(chrono::steady_clock::now());
        throw retry_operation("try again");
    }
    template<typename R, typename P>
    void function_with_delay(chrono::duration<R, P> delay)
    {
        this_thread::sleep_for(delay);
        function();
    }
    vector<chrono::microseconds> timing_differences()
    {
        vector<chrono::microseconds> retval;
        auto last = timings.front();
        for (auto& t : timings) {
            retval.push_back(chrono::duration_cast<chrono::microseconds>(t - last));
            last = t;
        }
        return retval;
    }
    chrono::milliseconds elapsed_ms()
    {
        return chrono::duration_cast<chrono::milliseconds>(timings.back() - timings.front());
    }
};
// convenience stuff
auto one_ms = chrono::milliseconds(1);
auto ten_ms = chrono::milliseconds(10);
auto hundred_ms = chrono::milliseconds(100);

TEST(ExpBackoffWithTimeout, WillTimeout)
{
    retry_state state;
    ASSERT_THROW(retry_op_exponential_backoff_timeout<void>(one_ms, ten_ms, hundred_ms, [&state] { state.function(); }),
                 retry_operation_timeout);
    // sleep_for is only guaranteed to sleep for _at_least_ the time requested.
    // so lets make sure the total elapsed time is at least what we wanted.
    ASSERT_GE(state.timings.size(), 0);
    ASSERT_GE(state.elapsed_ms(), hundred_ms);
}

TEST(ExpBackoffWithTimeout, RetryCountInRange)
{
    retry_state state;
    ASSERT_THROW(retry_op_exponential_backoff_timeout<void>(one_ms, ten_ms, hundred_ms, [&state] { state.function(); }),
                 retry_operation_timeout);
    // should be 1+2+4+8+10+10+10+...
    // +/- 10% jitter RECALCULATE if jitter fraction changes!
    // Consider solving exactly if we allow user-supplied jitter fraction.
    // So retries should be less than or equal 0.9+1.8+3.6+7.2+9+9.. = 13.5 + 9+...(9 times)+ 5.5 = 14
    // and greater than or equal 1.1+2.2+4.4+8.8+11+... = 16.5 + 11+11...(7 times)+ 6.5 = 12
    // the # times it will be called is one higher than this.  Also - since sleep_for can be _longer_
    // than you ask for, we could be significantly under the 12 above.  Lets just make sure they are not
    // more frequent than the max
    ASSERT_LE(state.timings.size(), 15);
}

TEST(ExpBackoffWithTimeout, RetryTimingReasonable)
{
    retry_state state;
    ASSERT_THROW(retry_op_exponential_backoff_timeout<void>(one_ms, ten_ms, hundred_ms, [&state] { state.function(); }),
                 retry_operation_timeout);
    // expect 0,1,2,4,8,10... +/-10% with last one being the remainder
    int count = 0;
    auto last = state.timings.size() - 1;
    for (auto& t : state.timing_differences()) {
        if (count == 0) {
            ASSERT_EQ(0, t.count());
        } else if (count < last) {
            // in microseconds
            auto min = min_jitter_fraction * pow(2, count - 1) * 1000.0;
            if (min < 10000) {
                ASSERT_GE(t.count(), min);
            } else {
                ASSERT_GE(t.count(), 10000);
            }
        }
        count++;
    }
}

TEST(ExpBackoffWithTimeout, AlwaysRetriesAtLeastOnce)
{
    retry_state state;
    ASSERT_THROW(retry_op_exponential_backoff_timeout<void>(ten_ms, ten_ms, ten_ms, [&state] { state.function(); }),
                 retry_operation_timeout);
    // Usually just retries once, sometimes the jitter means a second retry
    ASSERT_LE(2, state.timings.size());
}

TEST(ExpBackoffMaxAttempts, WillStopAtMax)
{
    retry_state state;
    ASSERT_THROW(retry_op_exponential_backoff<void>(one_ms, 20, [&state] { state.function(); }), retry_operation_retries_exhausted);
    // this will delay 1+2+4+8+16+32+128+128+128... = 255+128+128... = 7(to get to 255)+13*128
    ASSERT_EQ(21, state.timings.size());
}

TEST(ExpBackoffMaxAttempts, ZeroRetries)
{
    retry_state state;
    ASSERT_THROW(retry_op_exponential_backoff<void>(one_ms, 0, [&state] { state.function(); }), retry_operation_retries_exhausted);
    // Should always be called once
    ASSERT_EQ(1, state.timings.size());
}

TEST(ExpBackoffWithMaxAttempts, RetryTimingReasonable)
{
    retry_state state;
    ASSERT_THROW(retry_op_exponential_backoff<void>(one_ms, 10, [&state] { state.function(); }), retry_operation_retries_exhausted);
    // expect 0,1,2,4,8,16,32,64,128,128..... +/-10% with last one being the remainder
    int count = 0;
    auto last = state.timings.size() - 1;
    for (auto& t : state.timing_differences()) {
        if (count == 0) {
            ASSERT_EQ(0, t.count());
        } else if (count < last) {
            auto min = min_jitter_fraction * pow(2, fmin(DEFAULT_RETRY_OP_EXPONENT_CAP, count - 1)) * 1000;
            ASSERT_GE(t.count(), min);
        }
        count++;
    }
}
