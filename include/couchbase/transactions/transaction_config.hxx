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
#include <couchbase/support.hxx>
#include <couchbase/transactions/attempt_context_testing_hooks.hxx>
#include <couchbase/transactions/cleanup_testing_hooks.hxx>
#include <couchbase/transactions/durability_level.hxx>

namespace couchbase
{
namespace transactions
{
    /**
     * Tunables for the transactions.
     */
    class transaction_config
    {
      public:
        enum durability_level durability_level() const
        {
            return level_;
        }

        void durability_level(enum durability_level level)
        {
            level_ = level;
        }

        CB_NODISCARD std::chrono::milliseconds cleanup_window() const
        {
            return cleanup_window_;
        }

        template<typename T>
        void cleanup_window(T duration)
        {
            cleanup_window_ = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        }

        CB_NODISCARD std::chrono::nanoseconds expiration_time() const
        {
            return expiration_time_;
        }

        void cleanup_lost_attempts(bool value)
        {
            cleanup_lost_attempts_ = value;
        }

        CB_NODISCARD bool cleanup_lost_attempts() const
        {
            return cleanup_lost_attempts_;
        }

        void cleanup_client_attempts(bool value)
        {
            cleanup_client_attempts_ = value;
        }

        CB_NODISCARD bool cleanup_client_attempts() const
        {
            return cleanup_client_attempts_;
        }

        template<typename T>
        void expiration_time(T duration)
        {
            expiration_time_ = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        }

        void test_factories(attempt_context_testing_hooks& hooks, cleanup_testing_hooks& cleanup_hooks)
        {
            attempt_context_hooks_ = hooks;
            cleanup_hooks_ = cleanup_hooks;
        }

        CB_NODISCARD attempt_context_testing_hooks attempt_context_hooks() const
        {
            return attempt_context_hooks_;
        }

        CB_NODISCARD cleanup_testing_hooks cleanup_hooks() const
        {
            return cleanup_hooks_;
        }

      private:
        enum couchbase::transactions::durability_level level_{ couchbase::transactions::durability_level::MAJORITY };
        std::chrono::milliseconds cleanup_window_{ 120000 };
        std::chrono::nanoseconds expiration_time_{ std::chrono::seconds(15) };
        bool cleanup_lost_attempts_{ true };
        bool cleanup_client_attempts_{ true };
        attempt_context_testing_hooks attempt_context_hooks_;
        cleanup_testing_hooks cleanup_hooks_;
    };

} // namespace transactions
} // namespace couchbase
