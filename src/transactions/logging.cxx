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

#include "couchbase/transactions/internal/logging.hxx"
#include <couchbase/transactions.hxx>

namespace couchbase
{
namespace transactions
{
    std::shared_ptr<spdlog::logger> init_txn_log()
    {
        static std::shared_ptr<spdlog::logger> txnlogger = spdlog::stdout_logger_mt(TXN_LOG);
        return txnlogger;
    }

    std::shared_ptr<spdlog::logger> init_attempt_cleanup_log()
    {
        static auto txnlogger = spdlog::stdout_logger_mt(ATTEMPT_CLEANUP_LOG);
        return txnlogger;
    }
    std::shared_ptr<spdlog::logger> init_lost_attempts_log()
    {
        static auto txnlogger = spdlog::stdout_logger_mt(LOST_ATTEMPT_CLEANUP_LOG);
        return txnlogger;
    }

    // TODO: better integration with client, so we don't need to repeat this private
    // method.
    spdlog::level::level_enum translate_level(couchbase::logger::level level)
    {
        switch (level) {
            case couchbase::logger::level::trace:
                return spdlog::level::level_enum::trace;
            case couchbase::logger::level::debug:
                return spdlog::level::level_enum::debug;
            case couchbase::logger::level::info:
                return spdlog::level::level_enum::info;
            case couchbase::logger::level::warn:
                return spdlog::level::level_enum::warn;
            case couchbase::logger::level::err:
                return spdlog::level::level_enum::err;
            case couchbase::logger::level::critical:
                return spdlog::level::level_enum::critical;
            case couchbase::logger::level::off:
                return spdlog::level::level_enum::off;
        }
        return spdlog::level::level_enum::trace;
    }

    void set_transactions_log_level(couchbase::logger::level level)
    {
        spdlog::level::level_enum lvl = translate_level(level);
        txn_log->set_level(lvl);
        attempt_cleanup_log->set_level(lvl);
        lost_attempts_cleanup_log->set_level(lvl);
    }

} // namespace transactions
} // namespace couchbase
