#pragma once

#include <couchbase/transactions/durability_level.hxx>

namespace couchbase
{
namespace transactions
{
    /**
     * Tunables for the transactions.
     */
    class configuration
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

        void cleanup_window(int ms)
        {
            cleanup_window_ = ms;
        }

      private:
        enum durability_level level_ { durability_level::MAJORITY };
        int cleanup_window_{ 120000 };
    };

} // namespace transactions
} // namespace couchbase
