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

#include <chrono>
#include <string>
#include <vector>
#include <thread>

#include <couchbase/transactions/logging.hxx>
#include <couchbase/transactions/transaction_attempt.hxx>
#include <couchbase/transactions/transaction_config.hxx>
#include <couchbase/transactions/uid_generator.hxx>

namespace couchbase
{
namespace transactions
{
    class transaction_context
    {
      public:
        transaction_context()
          : transaction_id_(uid_generator::next())
          , start_time_client_(std::chrono::system_clock::now())
          , deferred_elapsed_(0)
        {
        }

        CB_NODISCARD const std::string& transaction_id() const
        {
            return transaction_id_;
        }

        CB_NODISCARD size_t num_attempts() const
        {
            return attempts_.size();
        }
        CB_NODISCARD const std::vector<transaction_attempt>& attempts() const
        {
            return attempts_;
        }
        CB_NODISCARD std::vector<transaction_attempt>& attempts()
        {
            return const_cast<std::vector<transaction_attempt>&>(const_cast<const transaction_context*>(this)->attempts());
        }
        CB_NODISCARD const transaction_attempt& current_attempt() const
        {
            if (attempts_.empty()) {
                throw std::runtime_error("transaction context has no attempts yet");
            }
            return attempts_.back();
        }
        CB_NODISCARD transaction_attempt& current_attempt()
        {
            return const_cast<transaction_attempt&>(const_cast<const transaction_context*>(this)->current_attempt());
        }

        void add_attempt()
        {
            transaction_attempt attempt{};
            attempts_.push_back(attempt);
        }
        CB_NODISCARD bool has_expired_client_side(const transaction_config& config)
        {
            const auto& now = std::chrono::system_clock::now();
            auto expired_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_time_client_) + deferred_elapsed_;
            auto expired_millis = std::chrono::duration_cast<std::chrono::milliseconds>(expired_nanos);
            bool is_expired = expired_nanos > config.expiration_time();
            if (is_expired) {
                spdlog::info("has expired client side (now={}ns, start={}ns, deferred_elapsed={}ns, expired={}ns ({}ms), config={}ms)",
                             now.time_since_epoch().count(),
                             start_time_client_.time_since_epoch().count(),
                             deferred_elapsed_.count(),
                             expired_nanos.count(),
                             expired_millis.count(),
                             std::chrono::duration_cast<std::chrono::milliseconds>(config.expiration_time()).count());
            }
            return is_expired;
        }

        void retry_delay(const transaction_config& config)
        {
            // when we retry an operation, we typically call that function recursively.  So, we need to
            // limit total number of times we do it.  Later we can be more sophisticated perhaps.
            auto delay = config.expiration_time() / 100; // the 100 is arbitrary
            spdlog::trace("about to sleep for {} ms", std::chrono::duration_cast<std::chrono::milliseconds>(delay).count());
            std::this_thread::sleep_for(delay);
        }

        CB_NODISCARD std::chrono::time_point<std::chrono::system_clock> start_time_client() const
        {
            return start_time_client_;
        }

        CB_NODISCARD const std::string atr_id() const
        {
            return atr_id_;
        }

        void atr_id(const std::string& id)
        {
            atr_id_ = id;
        }
        CB_NODISCARD std::string atr_collection() const
        {
            return atr_collection_;
        }
        void atr_collection(const std::string& coll)
        {
            atr_collection_ = coll;
        }

      private:
        std::string transaction_id_;

        /** The time this overall transaction started */
        const std::chrono::time_point<std::chrono::system_clock> start_time_client_;

        /**
         * Will be non-zero only when resuming a deferred transaction. It records how much time has elapsed in total in the deferred
         * transaction, including the time spent in the original transaction plus any time spent while deferred.
         */
        const std::chrono::nanoseconds deferred_elapsed_;

        std::vector<transaction_attempt> attempts_;

        std::string atr_id_;

        std::string atr_collection_;
    };
} // namespace transactions
} // namespace couchbase
