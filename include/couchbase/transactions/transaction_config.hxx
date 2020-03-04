#pragma once

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

        int cleanup_window() const
        {
            return cleanup_window_;
        }

        [[nodiscard]] std::chrono::nanoseconds transaction_expiration_time() const
        {
            return transaction_expiration_time_;
        }

      private:
        enum durability_level level_ { durability_level::MAJORITY };
        int cleanup_window_{ 120000 };
        std::chrono::nanoseconds transaction_expiration_time_{ std::chrono::seconds(15) };
    };

} // namespace transactions
} // namespace couchbase
