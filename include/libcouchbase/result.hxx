#pragma once

#include <string>
#include <vector>

#include <libcouchbase/couchbase.h>
#include <json11.hpp>

namespace couchbase
{

class result
{
  public:
    result();

    lcb_STATUS rc;
    uint64_t cas;
    uint8_t datatype;
    uint32_t flags;
    std::string key;
    json11::Json value;
    std::vector<json11::Json> values;
};

} // namespace couchbase
