#pragma once

#include <functional>
#include <couchbase/client/cluster.hxx>

#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/logging.hxx>
#include <couchbase/transactions/transaction_config.hxx>
#include <couchbase/transactions/transactions_cleanup.hxx>
#include <couchbase/transactions/transaction_result.hxx>

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
        transactions(cluster& cluster, const transaction_config& config)
          : cluster_(cluster)
          , config_(config)
          , cleanup_(cluster_, config_)
        {
        }

        transaction_result run(const logic& logic)
        {
            transaction_context overall;
            while (!overall.has_expired_client_side(config_)) {
                attempt_context ctx(this, overall, config_);
                spdlog::info("starting attempt {}/{}/{}", overall.num_attempts(), overall.transaction_id(), ctx.attempt_id());
                try {
                    logic(ctx);
                    if (!ctx.is_done()) {
                        ctx.commit();
                    }
                    break;
                } catch (const error_wrapper& er) {
                    spdlog::error("got error_wrapper {}", er.what());
                    if (er.should_retry()) {
                        spdlog::trace("got retryable exception {}, retrying", er.what());
                        continue;
                    }
                    if (er.should_rollback()) {
                        spdlog::trace("got non-retryable exception, rolling back");
                        ctx.rollback();
                    }

                    // throw the expected exception here
                    er.do_throw(overall);
                } catch (const std::runtime_error& ex) {
                    spdlog::error("got runtime error {}", ex.what());
                    ctx.rollback();
                    break;
                }
            }
            return transaction_result{overall.transaction_id(),
                                      overall.atr_id(),
                                      overall.atr_collection(),
                                      overall.attempts(),
                                      overall.current_attempt().state == attempt_state::COMPLETED};

        }

        void commit(attempt_context& ctx)
        {
            ctx.commit();
        }

        void rollback(attempt_context& ctx)
        {
            ctx.rollback();
        }

        void close()
        {
        }

        [[nodiscard]] transactions_cleanup& cleanup()
        {
            return cleanup_;
        }

      private:
        couchbase::cluster& cluster_;
        transaction_config config_;
        transactions_cleanup cleanup_;
    };
} // namespace transactions
} // namespace couchbase
