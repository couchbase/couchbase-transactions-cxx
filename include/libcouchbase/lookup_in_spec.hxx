#pragma once

#include <string>

namespace couchbase
{
class Collection;

enum LookupInSpecType { LOOKUP_IN_GET, LOOKUP_IN_FULLDOC_GET };

class LookupInSpec
{
    friend Collection;

  public:
    static LookupInSpec get(const std::string &path);
    static LookupInSpec fulldoc_get();

    LookupInSpec &xattr();

  private:
    LookupInSpecType type_;
    std::string path_;
    uint32_t flags_;

    LookupInSpec(LookupInSpecType type, const std::string &path);
};
} // namespace couchbase
