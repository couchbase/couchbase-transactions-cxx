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

#include <boost/optional.hpp>
#include <chrono>
#include <couchbase/support.hxx>
#include <couchbase/transactions/durability_level.hxx>
#include <memory>

namespace couchbase
{
namespace transactions
{
    /** @internal */
    class attempt_context_testing_hooks;

    /** @internal */
    class cleanup_testing_hooks;
    /**
     * @brief Configuration parameters for transactions.
     */
    class transaction_config
    {
      public:
        transaction_config();

        ~transaction_config();

        transaction_config(const transaction_config& c);

        transaction_config& operator=(const transaction_config& c);

        /**
         * @brief Get the default durability level for all transaction operations
         *
         * @return The default durability level used for write operations.
         */
        CB_NODISCARD couchbase::transactions::durability_level durability_level() const
        {
            return level_;
        }

        /**
         * @brief Set the default durability level for all transaction operations
         *
         * @param level The default durability level desired for write operations.
         */
        void durability_level(enum couchbase::transactions::durability_level level)
        {
            level_ = level;
        }

        /**
         * @brief Get cleanup window
         *
         * Each @ref transactions instance has background threads which looks for evidence of
         * transactions that somehow were not cleaned up during ordinary processing.  There is one
         * of these per bucket.  The thread looks through the active transaction records on that bucket
         * once during each window.  There are potentially 1024 of these records, so over one cleanup
         * window period, the thread will look for all 1024 of these, and examine any it finds.  Note
         * you can disable this by setting @ref cleanup_lost_attempts() false.
         *
         * @return The cleanup window.
         */
        CB_NODISCARD std::chrono::milliseconds cleanup_window() const
        {
            return cleanup_window_;
        }

        /**
         * @brief Set cleanup window
         *
         * @see cleanup_window() for more info.
         * @param duration An std::chrono::duration representing the cleanup window duration.
         */
        template<typename T>
        void cleanup_window(T duration)
        {
            cleanup_window_ = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        }

        /**
         * @brief Set kv_timeout
         *
         * @see kv_timeout()
         * @param duration An std::chrono::duration representing the desired default kv operation timeout.
         */
        template<typename T>
        void kv_timeout(T duration)
        {
            kv_timeout_ = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        }

        /**
         * @brief Get kv_timeout
         *
         * This is the default kv operation timeout used throughout the transactions.  Note all the operations
         * have an options class which allows you to override this value for a particular operation, if desired.
         *
         * @return The default kv operation timeout.
         */
        CB_NODISCARD boost::optional<std::chrono::milliseconds> kv_timeout() const
        {
            return kv_timeout_;
        }

        /**
         * @brief Get expiration time for transactions
         *
         * Transactions can conflict with eachother (or other operations on those documents), and may retry.
         * This is the maximum time a transaction can take, including any retries.  The transaction will throw
         * an @ref transaction_expired and rollback when this occurs.
         *
         * @return expiration time for transactions.
         */
        CB_NODISCARD std::chrono::nanoseconds expiration_time() const
        {
            return expiration_time_;
        }
        /**
         * @brief Set the expiration time for transactions.
         *
         * @param duration desired expiration for transactions. see @expiration_time().
         */
        template<typename T>
        void expiration_time(T duration)
        {
            expiration_time_ = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        }

        /**
         * @brief Enable/disable the lost attempts cleanup loop.
         * @see @ref cleanup_window() for description of the cleanup lost attempts loop.
         *
         * @param value If false, do not start the lost attempts cleanup threads.
         */
        void cleanup_lost_attempts(bool value)
        {
            cleanup_lost_attempts_ = value;
        }
        /**
         * @brief Get lost attempts cleanup loop status.
         * @see @ref cleanup_window() for description of the lost attempts cleanup loop.
         *
         * @return If false, no lost attempts cleanup threads will be launched.
         */
        CB_NODISCARD bool cleanup_lost_attempts() const
        {
            return cleanup_lost_attempts_;
        }

        /**
         * @brief Set state for the client attempts cleanup loop.
         * @see @ref cleanup_client_attempts()
         *
         * @param value If true, run the cleanup client attempts loop.
         */
        void cleanup_client_attempts(bool value)
        {
            cleanup_client_attempts_ = value;
        }

        /**
         * @brief Get state of client attempts cleanup loop.
         *
         * A transactions object will create a background thread to do any cleanup necessary
         * for the transactions it has attempted.  This can be disabled if set to false.
         *
         * @return true if the thread is enabled, false if not.
         */
        CB_NODISCARD bool cleanup_client_attempts() const
        {
            return cleanup_client_attempts_;
        }

        /** @internal */
        void test_factories(attempt_context_testing_hooks& hooks, cleanup_testing_hooks& cleanup_hooks);

        /** @internal */
        attempt_context_testing_hooks& attempt_context_hooks() const
        {
            return *attempt_context_hooks_;
        }

        /** @internal */
        cleanup_testing_hooks& cleanup_hooks() const
        {
            return *cleanup_hooks_;
        }

      protected:
        couchbase::transactions::durability_level level_;
        std::chrono::milliseconds cleanup_window_;
        std::chrono::nanoseconds expiration_time_;
        boost::optional<std::chrono::milliseconds> kv_timeout_;
        bool cleanup_lost_attempts_;
        bool cleanup_client_attempts_;
        std::unique_ptr<attempt_context_testing_hooks> attempt_context_hooks_;
        std::unique_ptr<cleanup_testing_hooks> cleanup_hooks_;
    };
} // namespace transactions
} // namespace couchbase
