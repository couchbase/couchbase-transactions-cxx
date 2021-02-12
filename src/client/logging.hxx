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
