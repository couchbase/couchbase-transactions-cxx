#pragma once

#include <libcouchbase/transactions/durability_level.hxx>

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

      private:
        enum durability_level level_ { durability_level::MAJORITY };
    };

} // namespace transactions
} // namespace couchbase
