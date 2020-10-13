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
    /** @brief Transaction logic should be contained in a lambda of this form */
    typedef std::function<void(attempt_context&)> logic;

    /**
     * @mainpage
     * A transaction consists of a lambda containing all the operations you wish to perform. The transactions object
     * yields a @ref arrempt_context which you use for those operations.  For example:
     *
     * @code{.cpp}
     * cluster c("couchbase://127.0.0.1", "Administrator", "password");
     * transaction_config config;
     * config.durability_level(transactions::durability_level::MAJORITY);
     * auto b = cluster.bucket("default");
     * auto coll = b->default_collection();
     * transactions txn(c, config);
     *
     * try {
     *     txn.run([&](transactions::attempt_context& ctx) {
     *         ctx.upsert(coll, "somekey", nlohmann::json::parse("{\"a\":\"thing\"}"));
     *         ctx.insert(coll, "someotherkey", nlohmann::json::parse("{"\a\":\"different thing\"}"));
     *     });
     *     cout << "txn successful" << endl;
     * } catch (const transaction_failed& failed) {
     *     cerr << "txn failed: " << failed.what() << endl;
     * } catch (const transaction_expired& expired) {
     *    cerr << "txn timed out" << expired.what() << endl;
     * }
     * @endcode
     *
     * this upserts a document, and inserts another into the default collection in the bucket named "default".  If
     * unsuccessful, it outputs some information about what the issue was.
     *
     * For a much mor detailed example, see @ref examples/game_server.cxx
     *
     * @example examples/game_server.cxx
     */
    class transactions
    {
      public:
        /**
         * @brief Create a transactions object.
         *
         * Creates a transactions object, which can be used to run transactions within the current thread.
         *
         * @param cluster The cluster to use for the transactions.
         * @param config The configuration parameters to use for the transactions.
         */
        transactions(couchbase::cluster& cluster, const transaction_config& config)
          : cluster_(cluster)
          , config_(config)
          , cleanup_(cluster_, config_)
        {
            spdlog::info("couchbase transactions {} creating new transaction object", VERSION_STR);
        }

        std::shared_ptr<transactions> clone(couchbase::cluster& new_cluster,
                                            std::shared_ptr<attempt_context_testing_hooks> new_hooks,
                                            std::shared_ptr<cleanup_testing_hooks> new_cleanup_hooks)
        {
            spdlog::info("couchbase transactions {} copying transaction object", VERSION_STR);
            transaction_config config = config_;
            config.test_factories(*new_hooks, *new_cleanup_hooks);
            return std::make_shared<transactions>(new_cluster, config);
        }

        /**
         * @brief Run a transaction
         *
         * Expects a lambda, which it calls with an @ref attempt_context reference to be used in the lambda for
         * the transaction operations.
         *
         * @param logic The lambda containing the transaction logic.  See @logic for the
         */
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
                    cleanup_.add_attempt(ctx);
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
                            cleanup_.add_attempt(ctx);
                            continue;
                        }
                    }

                    // throw the expected exception here
                    cleanup_.add_attempt(ctx);
                    er.do_throw(overall);
                    // if we don't throw, break here means no retry
                    break;
                } catch (const std::runtime_error& ex) {
                    spdlog::error("got runtime error {}", ex.what());
                    ctx.rollback();
                    cleanup_.add_attempt(ctx);
                    break;
                } catch(...) {
                    spdlog::error("got unexpected error, rolling back");
                    ctx.rollback();
                    cleanup_.add_attempt(ctx);
                    break;
                }
            }
            return transaction_result{ overall.transaction_id(),
                                       overall.atr_id(),
                                       overall.atr_collection(),
                                       overall.attempts(),
                                       overall.current_attempt().state == attempt_state::COMPLETED };
        }

        /** called internally - will likely move */
        void commit(attempt_context& ctx)
        {
            ctx.commit();
        }

        /** called internally - will likely move */
        void rollback(attempt_context& ctx)
        {
            ctx.rollback();
        }

        /**
         * This shuts down the transactions object
         *
         * The object cannot be used after this call.  Called in destructor, but
         * available to call sooner if needed
         */
        void close()
        {
            spdlog::info("closing transactions");
            cleanup_.close();
            spdlog::info("transactions closed");
        }

        CB_NODISCARD transaction_config& config()
        {
            return config_;
        }
        CB_NODISCARD const transactions_cleanup& cleanup() const
        {
            return cleanup_;
        }
        CB_NODISCARD transactions_cleanup& cleanup()
        {
            return cleanup_;
        }

        CB_NODISCARD couchbase::cluster& cluster_ref()
        {
            return cluster_;
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
