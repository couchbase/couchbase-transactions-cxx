#pragma once

#include <string>

namespace couchbase
{
namespace transactions
{
    class transaction_context
    {
      private:
        std::string id_;

      public:
        transaction_context();

        const std::string &id();
    };
} // namespace transactions
} // namespace couchbase
