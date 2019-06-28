#pragma once

#include <libcouchbase/transactions/durability_level.hxx>

namespace couchbase
{
namespace transactions
{
    /**
     * Tunables for the transactions.
     */
    class Configuration
    {
      public:
        DurabilityLevel durabilityLevel();
        uint32_t transactionExpirationTime();
    };
} // namespace transactions
} // namespace couchbase
