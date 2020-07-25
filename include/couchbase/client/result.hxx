#pragma once

#include <string>
#include <vector>

#include <boost/optional.hpp>
#include <nlohmann/json.hpp>

namespace couchbase
{
struct result {
    uint32_t rc;
    uint64_t cas;
    uint8_t datatype;
    uint32_t flags;
    std::string key;
    boost::optional<nlohmann::json> value;
    std::vector<boost::optional<nlohmann::json>> values;

    [[nodiscard]] std::string strerror() const;
    [[nodiscard]] bool is_not_found() const;
    [[nodiscard]] bool is_success() const;
    [[nodiscard]] bool is_value_too_large() const;
    [[nodiscard]] std::string to_string() const;
};
} // namespace couchbase
