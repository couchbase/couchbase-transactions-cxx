#pragma once

#include <string>
#include <vector>

#include <libcouchbase/couchbase.h>
#include <folly/dynamic.h>

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
    folly::dynamic value;
    std::vector<folly::dynamic> values;
};

} // namespace couchbase
