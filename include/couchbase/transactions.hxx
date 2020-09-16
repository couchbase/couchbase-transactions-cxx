/*
 *     Copyright 2020 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

#include <couchbase/client/cluster.hxx>
#include <functional>
#include <thread>
#include <cmath>

#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/logging.hxx>
#include <couchbase/transactions/transaction_config.hxx>
#include <couchbase/transactions/transaction_result.hxx>
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
        transactions(cluster& cluster, const transaction_config& config)
          : cluster_(cluster)
          , config_(config)
          , cleanup_(cluster_, config_)
        {
            spdlog::info("couchbase transactions {} creating new transaction object", VERSION_STR);
        }

        transaction_result run(const logic& logic)
        {
            transaction_context overall;
            while (overall.num_attempts() < max_attempts_) {
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
                    // first rollback if appropriate.  Almost always is.
                    if (er.should_rollback()) {
                        spdlog::trace("got rollback-able exception, rolling back");
                        ctx.rollback();
                    }
                    if (er.should_retry()) {
                        if (overall.num_attempts() < max_attempts_) {
                            spdlog::trace("got retryable exception, retrying");
                            // simple linear backoff with #of attempts
                            std::this_thread::sleep_for(min_retry_delay_ * pow(2, fmin(10, overall.num_attempts())));
                            continue;
                        }
                    }

                    // throw the expected exception here
                    er.do_throw(overall);
                } catch (const std::runtime_error& ex) {
                    spdlog::error("got runtime error {}", ex.what());
                    ctx.rollback();
                    break;
                } catch(...) {
                    spdlog::error("got unexpected error, rolling back");
                    ctx.rollback();
                    break;
                }
            }
            return transaction_result{ overall.transaction_id(),
                                       overall.atr_id(),
                                       overall.atr_collection(),
                                       overall.attempts(),
                                       overall.current_attempt().state == attempt_state::COMPLETED };
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

        CB_NODISCARD transactions_cleanup& cleanup()
        {
            return cleanup_;
        }

      private:
        couchbase::cluster& cluster_;
        transaction_config config_;
        transactions_cleanup cleanup_;
        // TODO: realistic max - this helps with tests as the expiration is 2 min and thats forever
        const int max_attempts_{10};
        const std::chrono::milliseconds min_retry_delay_{10};
    };
} // namespace transactions
} // namespace couchbase
