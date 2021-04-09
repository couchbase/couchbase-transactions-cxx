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

#include "logging.hxx"

namespace couchbase
{
namespace transactions
{
    // Repeated code as we don't want spdlog in public interface
    spdlog::level::level_enum cb_to_spdlog_level(log_level level)
    {
        switch (level) {
            case log_level::TRACE:
                return spdlog::level::trace;
            case log_level::DEBUG:
                return spdlog::level::debug;
            case log_level::INFO:
                return spdlog::level::info;
            case log_level::WARN:
                return spdlog::level::warn;
            case log_level::ERROR:
                return spdlog::level::err;
            case log_level::CRITICAL:
                return spdlog::level::critical;
            default:
                return spdlog::level::off;
        }
    }

    void set_transactions_log_level(log_level level)
    {
        spdlog::level::level_enum lvl = cb_to_spdlog_level(level);
        txn_log->set_level(lvl);
        attempt_cleanup_log->set_level(lvl);
        lost_attempts_cleanup_log->set_level(lvl);
    }

} // namespace transactions
} // namespace couchbase
