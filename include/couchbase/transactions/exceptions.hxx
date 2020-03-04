#pragma once

#include <stdexcept>

namespace couchbase
{
namespace transactions
{

    class transaction_failed : public std::runtime_error
    {
      public:
        explicit transaction_failed(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };

    class document_already_in_transaction : public std::runtime_error
    {
      public:
        explicit document_already_in_transaction(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };

    class attempt_expired : public std::runtime_error
    {
      public:
        explicit attempt_expired(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };

    class attempt_exception : public std::runtime_error
    {
      public:
        explicit attempt_exception(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };

    class client_error : public std::runtime_error
    {
      public:
        explicit client_error(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };
} // namespace transactions
} // namespace couchbase
