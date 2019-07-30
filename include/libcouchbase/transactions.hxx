#pragma once

#include <functional>

#include <libcouchbase/cluster.hxx>

#include <libcouchbase/transactions/configuration.hxx>
#include <libcouchbase/transactions/attempt_context.hxx>

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
        transactions(couchbase::cluster &cluster, configuration &configuration);
        void run(const logic &logic);
        void close();
    };
} // namespace transactions
} // namespace couchbase
