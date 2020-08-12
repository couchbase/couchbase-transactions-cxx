#pragma once

#include <couchbase/transactions/attempt_context_testing_hooks.hxx>
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

        [[nodiscard]] std::chrono::milliseconds cleanup_window() const
        {
            return cleanup_window_;
        }

        template<typename T>
        void cleanup_window(T duration)
        {
            cleanup_window_ = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        }

        [[nodiscard]] std::chrono::nanoseconds expiration_time() const
        {
            return expiration_time_;
        }

        void cleanup_lost_attempts(bool value)
        {
            cleanup_lost_attempts_ = value;
        }

        void cleanup_client_attempts(bool value)
        {
            cleanup_client_attempts_ = value;
        }

        template<typename T>
        void expiration_time(T duration)
        {
            expiration_time_ = std::chrono::duration_cast<std::chrono::milliseconds>(duration);
        }

        void test_factories(attempt_context_testing_hooks& hooks)
        {
            attempt_context_hooks_ = hooks;
        }

        [[nodiscard]] attempt_context_testing_hooks attempt_context_hooks() const
        {
            return attempt_context_hooks_;
        }

      private:
        enum couchbase::transactions::durability_level level_ { couchbase::transactions::durability_level::MAJORITY };
        std::chrono::milliseconds cleanup_window_{ 120000 };
        std::chrono::nanoseconds expiration_time_{ std::chrono::seconds(15) };
        bool cleanup_lost_attempts_{ true };
        bool cleanup_client_attempts_{ true };
        attempt_context_testing_hooks attempt_context_hooks_;
    };

} // namespace transactions
} // namespace couchbase
