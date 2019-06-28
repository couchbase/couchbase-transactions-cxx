#pragma once

#include <string>
#include <vector>

#include <libcouchbase/couchbase.h>

namespace couchbase
{

class Result
{
  public:
    Result();

    lcb_STATUS rc;
    uint64_t cas;
    uint8_t datatype;
    uint32_t flags;
    std::string key;
    std::string value;
    std::vector< std::string > values;
};

} // namespace couchbase
