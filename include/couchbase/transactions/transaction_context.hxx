#pragma once

#include <string>
#include <vector>

#include <boost/log/sources/logger.hpp>

#include <couchbase/transactions/transaction_attempt.hxx>
#include <couchbase/transactions/uid_generator.hxx>

namespace couchbase
{
namespace transactions
{
    class transaction_context
    {
      public:
        transaction_context()
          : id_(uid_generator::next())
        {
        }

        [[nodiscard]] const std::string& id()
        {
            return id_;
        }

        [[nodiscard]] size_t num_attempts() const
        {
            return attempts_.size();
        }

        [[nodiscard]] boost::log::sources::logger_mt& logger()
        {
            return logger_;
        }

      private:
        std::string id_;
        std::vector<transaction_attempt> attempts_;
        boost::log::sources::logger_mt logger_;
    };
} // namespace transactions
} // namespace couchbase
