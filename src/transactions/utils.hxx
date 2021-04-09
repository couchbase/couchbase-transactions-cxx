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
#pragma once
#include "exceptions_internal.hxx"
#include <chrono>
#include <couchbase/client/options.hxx>
#include <couchbase/client/result.hxx>
#include <couchbase/transactions/transaction_config.hxx>
#include <functional>
#include <limits>
#include <random>
#include <thread>

namespace couchbase
{
namespace transactions
{
    // returns the parsed server time from the result of a lookup_in_spec::get("$vbucket").xattr() call
    static uint64_t now_ns_from_vbucket(const nlohmann::json& vbucket)
    {
        std::string now_str = vbucket["HLC"]["now"];
        return stoull(now_str, nullptr, 10) * 1000000000;
    }

    static couchbase::durability_level durability(durability_level level)
    {
        switch (level) {
            case durability_level::NONE:
                return couchbase::durability_level::none;
            case durability_level::MAJORITY:
                return couchbase::durability_level::majority;
            case durability_level::MAJORITY_AND_PERSIST_TO_ACTIVE:
                return couchbase::durability_level::majority_and_persist_to_active;
            case durability_level::PERSIST_TO_MAJORITY:
                return couchbase::durability_level::persist_to_majority;
            default:
                // mimic java here
                return couchbase::durability_level::majority;
        }
    }

    template<typename T>
    T& wrap_option(T&& opt, const transaction_config& config)
    {
        if (config.kv_timeout()) {
            opt.timeout(*config.kv_timeout());
        }
        auto durable_opt = dynamic_cast<common_mutate_options<T>*>(&opt);
        if (nullptr != durable_opt) {
            opt.durability(durability(config.durability_level()));
        }
        return opt;
    }

    static void wrap_collection_call(result& res, std::function<void(result&)> call)
    {
        call(res);
        if (!res.is_success()) {
            throw client_error(res);
        }
        if (!res.values.empty() && !res.ignore_subdoc_errors) {
            for (auto v : res.values) {
                if (v.status != 0) {
                    throw client_error(res);
                }
            }
        }
    }

    static const std::chrono::milliseconds DEFAULT_RETRY_OP_DELAY = std::chrono::milliseconds(3);
    static const std::chrono::milliseconds DEFAULT_RETRY_OP_EXP_DELAY = std::chrono::milliseconds(1);
    static const size_t DEFAULT_RETRY_OP_MAX_RETRIES = 100;
    static const double RETRY_OP_JITTER = 0.1; // means +/- 10% for jitter.
    static const size_t DEFAULT_RETRY_OP_EXPONENT_CAP = 8;
    static double jitter()
    {
        static std::random_device rd;
        static std::mt19937 gen(rd());
        static std::uniform_real_distribution<> dist(1 - RETRY_OP_JITTER, 1 + RETRY_OP_JITTER);

        return dist(gen);
    }

    template<typename R, typename R1, typename P1, typename R2, typename P2, typename R3, typename P3>
    R retry_op_exponential_backoff_timeout(std::chrono::duration<R1, P1> initial_delay,
                                           std::chrono::duration<R2, P2> max_delay,
                                           std::chrono::duration<R3, P3> timeout,
                                           std::function<R()> func)
    {
        auto end_time = std::chrono::steady_clock::now() + timeout;
        uint32_t retries = 0;
        while (true) {
            try {
                return func();
            } catch (const retry_operation& e) {
                auto now = std::chrono::steady_clock::now();
                if (now > end_time) {
                    break;
                }
                auto delay = initial_delay * (jitter() * pow(2, retries++));
                if (delay > max_delay) {
                    delay = max_delay;
                }
                if (now + delay > end_time) {
                    std::this_thread::sleep_for(end_time - now);
                } else {
                    std::this_thread::sleep_for(delay);
                }
            }
        }
        throw retry_operation_timeout("timed out");
    }

    template<typename R, typename Rep, typename Period>
    R retry_op_exponential_backoff(std::chrono::duration<Rep, Period> delay, size_t max_retries, std::function<R()> func)
    {
        for (size_t retries = 0; retries <= max_retries; retries++) {
            try {
                return func();
            } catch (const retry_operation& e) {
                // 2^7 = 128, so max delay fixed at 128 * delay
                std::this_thread::sleep_for(delay * (jitter() * pow(2, fmin(DEFAULT_RETRY_OP_EXPONENT_CAP, retries))));
            }
        }
        throw retry_operation_retries_exhausted("retry_op hit max retries!");
    }

    template<typename R>
    R retry_op_exp(std::function<R()> func)
    {
        return retry_op_exponential_backoff<R>(DEFAULT_RETRY_OP_EXP_DELAY, DEFAULT_RETRY_OP_MAX_RETRIES, func);
    }

    template<typename R, typename Rep, typename Period>
    R retry_op_constant_delay(std::chrono::duration<Rep, Period> delay, size_t max_retries, std::function<R()> func)
    {
        for (size_t retries = 0; retries <= max_retries; retries++) {
            try {
                return func();
            } catch (const retry_operation& e) {
                std::this_thread::sleep_for(delay);
            }
        }
        throw retry_operation_retries_exhausted("retry_op hit max retries!");
    }

    template<typename R>
    R retry_op(std::function<R()> func)
    {
        return retry_op_constant_delay<R>(DEFAULT_RETRY_OP_DELAY, std::numeric_limits<size_t>::max(), func);
    }
} // namespace transactions
} // namespace couchbase
