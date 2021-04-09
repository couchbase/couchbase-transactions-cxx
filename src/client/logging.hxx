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
#include <couchbase/support.hxx>
#include <memory>
#include <spdlog/fmt/ostr.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

// To avoid static initialization order issues, #define instead of static const
#define CLIENT_LOGGER "client"
#define LOGGER_PATTERN "[%H:%M:%S.%e][%n][%l][t:%t] %v"
namespace couchbase
{
static std::shared_ptr<spdlog::logger>
init_client_logger()
{
    static std::shared_ptr<spdlog::logger> logger = spdlog::stdout_logger_mt(CLIENT_LOGGER);
    spdlog::set_pattern(LOGGER_PATTERN);
    return logger;
}
static std::shared_ptr<spdlog::logger> client_log = spdlog::get(CLIENT_LOGGER) ? spdlog::get(CLIENT_LOGGER) : init_client_logger();
} // namespace couchbase
