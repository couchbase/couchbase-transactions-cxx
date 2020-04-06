#pragma once

#include <stdexcept>

namespace couchbase
{
class document_not_found_error : public std::runtime_error
{
  public:
    explicit document_not_found_error(const std::string& what)
      : std::runtime_error(what)
    {
    }
};

class document_exists_error : public std::runtime_error
{
  public:
    explicit document_exists_error(const std::string& what)
      : std::runtime_error(what)
    {
    }
};

class path_not_found_error : public std::runtime_error
{
  public:
    explicit path_not_found_error(const std::string& what)
      : std::runtime_error(what)
    {
    }
};

class cas_mismatch_error : public std::runtime_error
{
  public:
    explicit cas_mismatch_error(const std::string& what)
      : std::runtime_error(what)
    {
    }
};
} // namespace couchbase
