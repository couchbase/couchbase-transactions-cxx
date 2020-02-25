#pragma once

namespace couchbase
{
namespace transactions
{
    enum class durability_level {
        /**
         * Durability settings are disabled.
         */
        NONE = 0x00,

        /**
         * Wait until each write is available in-memory on a majority of configured replicas, before continuing.
         */
        MAJORITY = 0x01,

        /**
         * Wait until each write is available in-memory on a majority of configured replicas, and also persisted to
         * disk on the master node, before continuing.
         */
        MAJORITY_AND_PERSIST_TO_ACTIVE = 0x02,

        /**
         * Wait until each write is both available in-memory and persisted to disk on a majority of configured
         * replicas, and also, before continuing.
         */
        PERSIST_TO_MAJORITY = 0x03
    };
} // namespace transactions
} // namespace couchbase
