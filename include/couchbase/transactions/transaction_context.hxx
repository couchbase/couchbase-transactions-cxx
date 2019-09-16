#pragma once

#include <string>
#include <vector>

#include <boost/log/sources/logger.hpp>

#include <couchbase/transactions/transaction_attempt.hxx>

namespace couchbase
{
namespace transactions
{
    class transaction_context
    {
      public:
        transaction_context();

        const std::string &id();
        [[nodiscard]] size_t num_attempts() const;
        [[nodiscard]] boost::log::sources::logger_mt &logger();

      private:
        std::string id_;
        std::vector<transaction_attempt> attempts_;
        boost::log::sources::logger_mt logger_;
    };
} // namespace transactions
} // namespace couchbase
