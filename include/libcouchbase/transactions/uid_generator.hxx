#pragma once

#include <string>
namespace couchbase
{
namespace transactions
{
    class uid_generator
    {
      public:
        static std::string next();
    };
} // namespace transactions
} // namespace couchbase
