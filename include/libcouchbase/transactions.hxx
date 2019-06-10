#pragma once

#include <libcouchbase/cluster.hxx>

#include <libcouchbase/transactions/configuration.hxx>
#include <libcouchbase/transactions/logic.hxx>

namespace couchbase
{
namespace transactions
{
    /**
     * @mainpage
     *
     * @ref examples/game_server.cxx - Shows how the transaction could be integrated into application
     *
     * @example examples/game_server.cxx
     * Shows how the transaction could be integrated into application
     */
    class Transactions
    {
      public:
        Transactions(couchbase::Cluster &cluster, Configuration &configuration);
        void close();
        Result run(Logic &logic);
    };
} // namespace transactions
} // namespace couchbase
