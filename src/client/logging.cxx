#include "logging.hxx"

namespace couchbase
{

spdlog::level::level_enum
cb_to_spdlog_level(log_levels level)
{
    switch (level) {
        case log_levels::TRACE:
            return spdlog::level::trace;
        case log_levels::DEBUG:
            return spdlog::level::debug;
        case log_levels::INFO:
            return spdlog::level::info;
        case log_levels::WARN:
            return spdlog::level::warn;
        case log_levels::ERROR:
            return spdlog::level::err;
        case log_levels::CRITICAL:
            return spdlog::level::critical;
        default:
            return spdlog::level::off;
    }
}

void
set_client_log_level(log_levels level)
{
    client_log->set_level(cb_to_spdlog_level(level));
}

} // namespace couchbase
