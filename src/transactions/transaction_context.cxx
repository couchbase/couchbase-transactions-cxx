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

#include "transaction_context.hxx"
#include "logging.hxx"
#include "uid_generator.hxx"

namespace couchbase
{
namespace transactions
{
    transaction_context::transaction_context(transactions& txns)
      : transaction_id_(uid_generator::next())
      , transactions_(txns)
      , config_(txns.config())
      , start_time_client_(std::chrono::steady_clock::now())
      , deferred_elapsed_(0)
      , cleanup_(txns.cleanup())
    {
    }

    void transaction_context::add_attempt()
    {
        transaction_attempt attempt{};
        attempts_.push_back(attempt);
    }

    CB_NODISCARD bool transaction_context::has_expired_client_side(const transaction_config& config)
    {
        const auto& now = std::chrono::steady_clock::now();
        auto expired_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_time_client_) + deferred_elapsed_;
        auto expired_millis = std::chrono::duration_cast<std::chrono::milliseconds>(expired_nanos);
        bool is_expired = expired_nanos > config.expiration_time();
        if (is_expired) {
            txn_log->info("has expired client side (now={}ns, start={}ns, deferred_elapsed={}ns, expired={}ns ({}ms), config={}ms)",
                          now.time_since_epoch().count(),
                          start_time_client_.time_since_epoch().count(),
                          deferred_elapsed_.count(),
                          expired_nanos.count(),
                          expired_millis.count(),
                          std::chrono::duration_cast<std::chrono::milliseconds>(config.expiration_time()).count());
        }
        return is_expired;
    }

    void transaction_context::retry_delay()
    {
        // when we retry an operation, we typically call that function recursively.  So, we need to
        // limit total number of times we do it.  Later we can be more sophisticated, perhaps.
        auto delay = config_.expiration_time() / 100; // the 100 is arbitrary
        txn_log->trace("about to sleep for {} ms", std::chrono::duration_cast<std::chrono::milliseconds>(delay).count());
        std::this_thread::sleep_for(delay);
    }

} // namespace transactions
} // namespace couchbase
