#pragma once

#include <functional>

#include <couchbase/client/cluster.hxx>

#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/configuration.hxx>
#include <couchbase/transactions/logging.hxx>
#include <couchbase/transactions/transactions_cleanup.hxx>

namespace couchbase
{
namespace transactions
{
    typedef std::function<void(attempt_context&)> logic;

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
        transactions(cluster& cluster, const configuration& config)
          : cluster_(cluster)
          , config_(config)
          , cleanup_(cluster_, config_)
        {
        }

        void run(const logic& logic)
        {
            transaction_context overall;
            attempt_context ctx(overall, config_);
            LOG(overall, info) << "starting attempt " << overall.num_attempts() << "/" << overall.id() << "/" << ctx.id();
            logic(ctx);
            if (!ctx.is_done()) {
                ctx.commit();
            }
        }
        void close()
        {
        }

      private:
        couchbase::cluster& cluster_;
        const configuration& config_;
        transactions_cleanup cleanup_;
    };
} // namespace transactions
} // namespace couchbase
