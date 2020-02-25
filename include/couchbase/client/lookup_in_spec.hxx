#pragma once

#include <libcouchbase/couchbase.h>
#include <string>

namespace couchbase
{
class collection;

enum class lookup_in_spec_type { LOOKUP_IN_GET, LOOKUP_IN_FULLDOC_GET };

class lookup_in_spec
{
    friend collection;

  public:
    static lookup_in_spec get(const std::string& path)
    {
        return lookup_in_spec(lookup_in_spec_type::LOOKUP_IN_GET, path);
    }

    static lookup_in_spec fulldoc_get()
    {
        return lookup_in_spec(lookup_in_spec_type::LOOKUP_IN_FULLDOC_GET);
    }

    lookup_in_spec& xattr()
    {
        flags_ |= LCB_SUBDOCSPECS_F_XATTRPATH;
        return *this;
    }

  private:
    lookup_in_spec_type type_;
    std::string path_;
    uint32_t flags_;

    explicit lookup_in_spec(lookup_in_spec_type type, std::string path = "")
      : type_(type)
      , path_(std::move(path))
      , flags_(0)
    {
    }
};
} // namespace couchbase
