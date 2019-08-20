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
        enum durability_level durability_level() const;
        void durability_level(enum durability_level level);

        int cleanup_window() const;
        void cleanup_window(int ms);

      private:
        enum durability_level level_ { durability_level::MAJORITY };
        int cleanup_window_{ 120000 };
    };

} // namespace transactions
} // namespace couchbase
