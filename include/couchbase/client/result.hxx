#pragma once

#include <string>
#include <vector>

#include <boost/optional.hpp>
#include <libcouchbase/couchbase.h>
#include <nlohmann/json.hpp>

namespace couchbase
{
struct result {
    lcb_STATUS rc;
    uint64_t cas;
    uint8_t datatype;
    uint32_t flags;
    std::string key;
    boost::optional<nlohmann::json> value;
    std::vector<boost::optional<nlohmann::json>> values;
};
} // namespace couchbase
