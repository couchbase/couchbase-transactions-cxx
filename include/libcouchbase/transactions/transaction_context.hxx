#pragma once

#include <string>

namespace couchbase
{
namespace transactions
{

    class TransactionContext
    {
      private:
        std::string id_;

      public:
        TransactionContext();

        const std::string &id();
    };

} // namespace transactions
} // namespace couchbase
