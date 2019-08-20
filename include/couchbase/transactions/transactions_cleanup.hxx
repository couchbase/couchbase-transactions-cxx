#pragma once

#include <couchbase/client/cluster.hxx>
#include <couchbase/transactions/configuration.hxx>

#include <thread>

namespace couchbase
{
namespace transactions
{
    class transactions_cleanup
    {
      public:
        transactions_cleanup(cluster &cluster, const configuration &config);
        ~transactions_cleanup();

      private:
        void lost_attempts_loop();

        cluster &cluster_;
        const configuration &config_;

        std::thread lost_attempts_thr;
        const std::string client_uuid_;

        bool running { false };
    };
} // namespace transactions
} // namespace couchbase
