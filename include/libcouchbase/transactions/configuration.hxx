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
        durability_level durabilityLevel();
        uint32_t transaction_expiration_time();
    };
} // namespace transactions
} // namespace couchbase
