#pragma once

#include <string>
namespace couchbase
{
namespace transactions
{
    class UidGenerator
    {
      public:
        static std::string next();
    };
} // namespace transactions
} // namespace couchbase
