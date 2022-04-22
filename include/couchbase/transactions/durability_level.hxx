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
        return "MAJORITY";
    }

    static std::string durability_level_to_string_for_query(durability_level l)
    {
        switch (l) {
            case durability_level::NONE:
                return "none";
            case durability_level::MAJORITY:
                return "majority";
            case durability_level::MAJORITY_AND_PERSIST_TO_ACTIVE:
                return "majorityAndPersistToActive";
            case durability_level::PERSIST_TO_MAJORITY:
                return "persistToMajority";
        }
        return "majority";
    }

    static std::string store_durability_level_to_string(durability_level l)
    {
        switch (l) {
            case durability_level::NONE:
                return "n";
            case durability_level::MAJORITY:
                return "m";
            case durability_level::MAJORITY_AND_PERSIST_TO_ACTIVE:
                return "pa";
            case durability_level::PERSIST_TO_MAJORITY:
                return "pm";
            default:
                return "m";
        }
    }

    static durability_level store_string_to_durability_level(const std::string& s)
    {
        if (s == "m") {
            return durability_level::MAJORITY;
        }
        if (s == "pa") {
            return durability_level::MAJORITY_AND_PERSIST_TO_ACTIVE;
        }
        if (s == "pm") {
            return durability_level::PERSIST_TO_MAJORITY;
        }
        if (s == "n") {
            return durability_level::NONE;
        }
        // Default to a something sensible if we don't understand the code
        return durability_level::MAJORITY;
    }
} // namespace transactions
} // namespace couchbase
