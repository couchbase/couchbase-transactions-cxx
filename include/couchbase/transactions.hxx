#pragma once

#include <functional>

#include <couchbase/client/cluster.hxx>

#include <couchbase/transactions/configuration.hxx>
#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/transactions_cleanup.hxx>

namespace couchbase
{
namespace transactions
{
    typedef std::function<void(attempt_context &)> logic;

    /**
     * @mainpage
     *
     * @ref examples/game_server.cxx - Shows how the transaction could be integrated into application
     *
     * @example examples/game_server.cxx
     * Shows how the transaction could be integrated into application
     */
    class transactions
    {
      public:
        transactions(cluster &cluster, const configuration &config);
        void run(const logic &logic);
        void close();

      private:
        couchbase::cluster &cluster_;
        const configuration &config_;
        transactions_cleanup cleanup_;
    };
} // namespace transactions
} // namespace couchbase
