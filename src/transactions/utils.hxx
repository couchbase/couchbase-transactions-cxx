/*
 *     Copyright 2020 Couchbase, Inc.
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
#include <couchbase/client/result.hxx>
#include <couchbase/transactions/exceptions.hxx>

#include <chrono>
#include <functional>
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

    static const std::chrono::milliseconds DEFAULT_RETRY_OP_DELAY = std::chrono::milliseconds(50);
    static const std::chrono::milliseconds DEFAULT_RETRY_OP_TIMEOUT = std::chrono::milliseconds(500);

    template<typename R>
    R retry_op(std::function<R()> func)
    {
        auto end_time = std::chrono::system_clock::now() + DEFAULT_RETRY_OP_TIMEOUT;

        while (std::chrono::system_clock::now() < end_time) {
            try {
                return func();
            } catch (const retry_operation& e) {
                auto now = std::chrono::system_clock::now();
                if (now + DEFAULT_RETRY_OP_DELAY > end_time) {
                    std::this_thread::sleep_for(end_time - now);
                } else {
                    std::this_thread::sleep_for(DEFAULT_RETRY_OP_DELAY);
                }
            }
        }
        return R{};
    }

} // namespace transactions
} // namespace couchbase
