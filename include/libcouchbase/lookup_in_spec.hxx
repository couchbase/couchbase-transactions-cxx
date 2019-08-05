#pragma once

#include <string>

namespace couchbase
{
class collection;

enum lookup_in_spec_type { LOOKUP_IN_GET, LOOKUP_IN_FULLDOC_GET };

class lookup_in_spec
{
    friend collection;

  public:
    static lookup_in_spec get(const std::string &path);
    static lookup_in_spec fulldoc_get();

    lookup_in_spec &xattr();

  private:
    lookup_in_spec_type type_;
    std::string path_;
    uint32_t flags_;

    lookup_in_spec(lookup_in_spec_type type, std::string path);
};
} // namespace couchbase
