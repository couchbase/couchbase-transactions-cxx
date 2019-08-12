#pragma once

#include <exception>

namespace couchbase
{
class not_found_error : public std::runtime_error
{
  public:
    explicit not_found_error(const std::string &what) : std::runtime_error(what)
    {
    }
};
} // namespace couchbase
