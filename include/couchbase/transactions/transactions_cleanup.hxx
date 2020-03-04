#pragma once

#include <couchbase/client/cluster.hxx>
#include <couchbase/transactions/transaction_config.hxx>

#include <thread>

namespace couchbase
{
class cluster;

namespace transactions
{
    class transactions_cleanup
    {
      public:
        transactions_cleanup(couchbase::cluster& cluster, const transaction_config& config);
        ~transactions_cleanup();

        [[nodiscard]] couchbase::cluster& cluster()
        {
            return cluster_;
        };

      private:
        void lost_attempts_loop();

        couchbase::cluster& cluster_;
        const transaction_config& config_;

        std::thread lost_attempts_thr;
        const std::string client_uuid_;

        bool running{ false };
    };
} // namespace transactions
} // namespace couchbase
