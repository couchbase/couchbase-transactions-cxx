/*
 *     Copyright 2021 Couchbase, Inc.
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
    static std::string durability_level_to_string(durability_level l)
    {
        switch (l) {
            case durability_level::NONE:
                return "NONE";
            case durability_level::MAJORITY:
                return "MAJORITY";
            case durability_level::MAJORITY_AND_PERSIST_TO_ACTIVE:
                return "MAJORITY_AND_PERSIST_TO_ACTIVE";
            case durability_level::PERSIST_TO_MAJORITY:
                return "PERSIST_TO_MAJORITY";
        }
    }
} // namespace transactions
} // namespace couchbase
