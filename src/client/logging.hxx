#pragma once
#include <couchbase/support.hxx>
#include <memory>
#include <spdlog/fmt/ostr.h>
#include <spdlog/sinks/stdout_sinks.h>
#include <spdlog/spdlog.h>

// To avoid static initialization order issues, #define instead of static const
#define CLIENT_LOGGER "client"
namespace couchbase
{
static std::shared_ptr<spdlog::logger> client_log =
  spdlog::get(CLIENT_LOGGER) ? spdlog::get(CLIENT_LOGGER) : spdlog::stdout_logger_mt(CLIENT_LOGGER);
} // namespace couchbase
