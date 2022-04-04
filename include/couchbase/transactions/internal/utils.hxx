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
#include "../../../../src/transactions/result.hxx"
#include "couchbase/transactions/internal/exceptions_internal.hxx"
#include <chrono>
#include <couchbase/errors.hxx>
#include <couchbase/operations.hxx>
#include <couchbase/operations/management/bucket_get_all.hxx>
#include <couchbase/transactions/transaction_config.hxx>
#include <functional>
#include <future>
#include <limits>
#include <random>
#include <string>
#include <thread>

namespace couchbase
{
namespace transactions
{
    /**
     * @brief Useful macros
     *
     * When mutating a document, if you need to store the cas of the document, the sequence number, or a crc of it,
     * you can use these macros and the server does it for you when it stores the mutation.  The server calculates
     * these after all the mutations in the operation.
     */
    namespace mutate_in_macro
    {
        /** @brief this expands to be the current document CAS (after the mutation) */
        static const std::string CAS = "\"${Mutation.CAS}\"";
        /** @brief this macro expands to the current sequence number (rev) of the document (after the mutation)*/
        static const std::string SEQ_NO = "\"${Mutation.seqno}\"";
        /** @brief this macro expands to a CRC32C of the document (after the mutation)*/
        static const std::string VALUE_CRC_32C = "\"${Mutation.value_crc32c}\"";
    }; // namespace mutate_in_macro

    // returns the parsed server time from the result of a lookup_in_spec::get("$vbucket").xattr() call
    static inline uint64_t now_ns_from_vbucket(const nlohmann::json& vbucket)
    {
        std::string now_str = vbucket["HLC"]["now"];
        return stoull(now_str, nullptr, 10) * 1000000000;
    }

    static inline std::string jsonify(const nlohmann::json& obj)
    {
        return obj.dump();
    }

    static inline std::string collection_spec_from_id(const couchbase::document_id& id)
    {
        std::string retval = id.scope();
        return retval.append(".").append(id.collection());
    }

    static inline bool document_ids_equal(const couchbase::document_id& id1, const couchbase::document_id& id2)
    {
        return id1.key() == id2.key() && id1.bucket() == id2.bucket() && id1.scope() == id2.scope() && id1.collection() == id2.collection();
    }

    template<typename OStream>
    OStream& operator<<(OStream& os, const couchbase::document_id& id)
    {
        os << "document_id{bucket: " << id.bucket() << ", scope: " << id.scope() << ", collection: " << id.collection()
           << ", key: " << id.key() << "}";
        return os;
    }

    static inline protocol::durability_level durability(durability_level level)
    {
        switch (level) {
            case durability_level::NONE:
                return protocol::durability_level::none;
            case durability_level::MAJORITY:
                return protocol::durability_level::majority;
            case durability_level::MAJORITY_AND_PERSIST_TO_ACTIVE:
                return protocol::durability_level::majority_and_persist_to_active;
            case durability_level::PERSIST_TO_MAJORITY:
                return protocol::durability_level::persist_to_majority;
            default:
                // mimic java here
                return protocol::durability_level::majority;
        }
    }

    template<typename T>
    T& wrap_request(T&& req, const transaction_config& config)
    {
        if (config.kv_timeout()) {
            req.timeout = config.kv_timeout().value();
        }
        return req;
    }

    template<typename T>
    T& wrap_durable_request(T&& req, const transaction_config& config)
    {
        wrap_request(req, config);
        req.durability_level = durability(config.durability_level());
        return req;
    }

    template<typename T>
    T& wrap_durable_request(T&& req, const transaction_config& config, durability_level dl)
    {
        wrap_request(req, config);
        req.durability_level = durability(dl);
        return req;
    }

    static inline result wrap_operation_future(std::future<result>& fut, bool ignore_subdoc_errors = true)
    {
        auto res = fut.get();
        if (!res.is_success()) {
            throw client_error(res);
        }
        // we should raise here, as we are doing a non-subdoc request and can't specify
        // access_deleted.  TODO: consider changing client to return document_not_found
        if (res.is_deleted && res.values.empty()) {
            res.ec = couchbase::error::key_value_errc::document_not_found;
            throw client_error(res);
        }
        if (!res.values.empty() && !ignore_subdoc_errors) {
            for (auto v : res.values) {
                if (v.status != subdoc_result::status_type::success) {
                    throw client_error(res);
                }
            }
        }
        return res;
    }

    static inline void wrap_collection_call(result& res, std::function<void(result&)> call)
    {
        call(res);
        if (!res.is_success()) {
            throw client_error(res);
        }
        if (!res.values.empty() && !res.ignore_subdoc_errors) {
            for (auto v : res.values) {
                if (v.status != subdoc_result::status_type::success) {
                    throw client_error(res);
                }
            }
        }
    }
    template<typename Resp>
    static bool is_error(const Resp& resp)
    {
        return !!resp.ctx.ec;
    }
    template<>
    bool is_error(const couchbase::operations::mutate_in_response& resp)
    {
        return resp.ctx.ec || resp.first_error_index;
    }

    template<typename Resp>
    static std::optional<error_class> error_class_from_response_extras(const Resp&)
    {
        return {};
    }

    template<>
    std::optional<error_class> error_class_from_response_extras(const couchbase::operations::mutate_in_response& resp)
    {
        if (!resp.first_error_index) {
            return {};
        }
        auto status = resp.fields.at(*resp.first_error_index).status;
        if (status == couchbase::protocol::status::subdoc_path_not_found) {
            return FAIL_PATH_NOT_FOUND;
        }
        if (status == couchbase::protocol::status::subdoc_path_exists) {
            return FAIL_PATH_ALREADY_EXISTS;
        }
        return FAIL_OTHER;
    }

    template<typename Resp>
    static std::optional<error_class> error_class_from_response(const Resp& resp)
    {
        if (!is_error(resp)) {
            return {};
        }
        if (resp.ctx.ec == couchbase::error::key_value_errc::document_not_found) {
            return FAIL_DOC_NOT_FOUND;
        }
        if (resp.ctx.ec == couchbase::error::key_value_errc::document_exists) {
            return FAIL_DOC_ALREADY_EXISTS;
        }
        if (resp.ctx.ec == couchbase::error::common_errc::cas_mismatch) {
            return FAIL_CAS_MISMATCH;
        }
        if (resp.ctx.ec == couchbase::error::key_value_errc::value_too_large) {
            return FAIL_ATR_FULL;
        }
        if (resp.ctx.ec == couchbase::error::common_errc::unambiguous_timeout ||
            resp.ctx.ec == couchbase::error::common_errc::temporary_failure ||
            resp.ctx.ec == couchbase::error::key_value_errc::durable_write_in_progress) {
            return FAIL_TRANSIENT;
        }
        if (resp.ctx.ec == couchbase::error::key_value_errc::durability_ambiguous ||
            resp.ctx.ec == couchbase::error::common_errc::ambiguous_timeout ||
            resp.ctx.ec == couchbase::error::common_errc::request_canceled) {
            return FAIL_AMBIGUOUS;
        }
        if (resp.ctx.ec == couchbase::error::key_value_errc::path_not_found) {
            return FAIL_PATH_NOT_FOUND;
        }
        if (resp.ctx.ec == couchbase::error::key_value_errc::path_exists) {
            return FAIL_PATH_ALREADY_EXISTS;
        }
        if (resp.ctx.ec) {
            return FAIL_OTHER;
        }
        return error_class_from_response_extras(resp);
    }

    static const std::chrono::milliseconds DEFAULT_RETRY_OP_DELAY = std::chrono::milliseconds(3);
    static const std::chrono::milliseconds DEFAULT_RETRY_OP_EXP_DELAY = std::chrono::milliseconds(1);
    static const size_t DEFAULT_RETRY_OP_MAX_RETRIES = 100;
    static const double RETRY_OP_JITTER = 0.1; // means +/- 10% for jitter.
    static const size_t DEFAULT_RETRY_OP_EXPONENT_CAP = 8;
    static inline double jitter()
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

    struct exp_delay {
        std::chrono::nanoseconds initial_delay;
        std::chrono::nanoseconds max_delay;
        std::chrono::nanoseconds timeout;
        mutable uint32_t retries;
        mutable std::optional<std::chrono::time_point<std::chrono::steady_clock>> end_time;

        template<typename R1, typename P1, typename R2, typename P2, typename R3, typename P3>
        exp_delay(std::chrono::duration<R1, P1> initial, std::chrono::duration<R2, P2> max, std::chrono::duration<R3, P3> limit)
          : initial_delay(std::chrono::duration_cast<std::chrono::nanoseconds>(initial))
          , max_delay(std::chrono::duration_cast<std::chrono::nanoseconds>(max))
          , timeout(std::chrono::duration_cast<std::chrono::nanoseconds>(limit))
          , retries(0)
          , end_time()
        {
        }
        void operator()() const
        {
            auto now = std::chrono::steady_clock::now();
            if (!end_time) {
                end_time = std::chrono::steady_clock::now() + timeout;
                return;
            }
            if (now > *end_time) {
                throw retry_operation_timeout("timed out");
            }
            auto delay = initial_delay * (jitter() * pow(2, retries++));
            if (delay > max_delay) {
                delay = max_delay;
            }
            if (now + delay > *end_time) {
                std::this_thread::sleep_for(*end_time - now);
            } else {
                std::this_thread::sleep_for(delay);
            }
        }
    };

    template<typename R, typename P>
    struct constant_delay {
        std::chrono::duration<R, P> delay;
        size_t max_retries;
        size_t retries;

        constant_delay(std::chrono::duration<R, P> d = DEFAULT_RETRY_OP_DELAY, size_t max = DEFAULT_RETRY_OP_MAX_RETRIES)
          : delay(d)
          , max_retries(max)
          , retries(0)
        {
        }
        void operator()()
        {
            if (retries++ >= max_retries) {
                throw retry_operation_retries_exhausted("retries exhausted");
            }
            std::this_thread::sleep_for(delay);
        }
    };

    static std::list<std::string> get_and_open_buckets(cluster& c)
    {
        couchbase::operations::management::bucket_get_all_request req{};
        // don't wrap this one, as the kv timeout isn't appropriate here.
        auto mtx = std::make_shared<std::mutex>();
        auto cv = std::make_shared<std::condition_variable>();
        size_t count = 1; // non-zero so we know not to stop waiting immediately
        std::list<std::string> bucket_names;
        c.execute(req, [cv, &bucket_names, &c, mtx, &count](couchbase::operations::management::bucket_get_all_response resp) {
            std::unique_lock<std::mutex> lock(*mtx);
            // now set count to correct # of buckets to try to open
            count = resp.buckets.size();
            lock.unlock();
            for (auto& b : resp.buckets) {
                // open the bucket
                c.open_bucket(b.name, [cv, name = b.name, &bucket_names, mtx, &count](std::error_code ec) {
                    std::unique_lock<std::mutex> lock(*mtx);
                    if (!ec) {
                        // push bucket name into list only if we successfully opened it
                        bucket_names.push_back(name);
                    }
                    if (--count == 0) {
                        cv->notify_all();
                    }
                    lock.unlock();
                });
            }
        });
        std::unique_lock<std::mutex> lock(*mtx);
        cv->wait(lock);
        return bucket_names;
    }
} // namespace transactions
} // namespace couchbase
