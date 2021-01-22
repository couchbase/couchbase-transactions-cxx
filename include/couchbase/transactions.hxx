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

#include <cmath>
#include <functional>
#include <thread>

#include <couchbase/client/cluster.hxx>
#include <couchbase/transactions/attempt_context.hxx>
#include <couchbase/transactions/exceptions.hxx>
#include <couchbase/transactions/transaction_config.hxx>
#include <couchbase/transactions/transaction_result.hxx>

namespace couchbase
{
namespace transactions
{
    /** @internal
     */
    class transactions_cleanup;

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
        transactions(couchbase::cluster& cluster, const transaction_config& config);

        /**
         * @brief Destructor
         */
        ~transactions();

        std::shared_ptr<transactions> clone(couchbase::cluster& new_cluster,
                                            std::shared_ptr<attempt_context_testing_hooks> new_hooks,
                                            std::shared_ptr<cleanup_testing_hooks> new_cleanup_hooks);

        /**
         * @brief Run a transaction
         *
         * Expects a lambda, which it calls with an @ref attempt_context reference to be used in the lambda for
         * the transaction operations.
         *
         * @param logic The lambda containing the transaction logic.
         * @return A struct containing some internal state information about the transaction.
         * @throws @ref transaction_failed, @ref transaction_expired, @ref transaction_commit_ambiguous, all of which
         *         share a common base class @ref transaction_exception.
         */
        transaction_result run(const logic& logic);

        /**
         * @internal
         * called internally - will likely move
         */
        void commit(attempt_context& ctx)
        {
            ctx.commit();
        }

        /**
         * @internal
         * called internally - will likely move
         */
        void rollback(attempt_context& ctx)
        {
            ctx.rollback();
        }

        /**
         * @brief Shut down the transactions object
         *
         * The transaction object cannot be used after this call.  Called in destructor, but
         * available to call sooner if needed.
         */
        void close();

        /**
         * @brief Return reference to @ref transaction_config.
         *
         * @return config for this transactions instance.
         */
        CB_NODISCARD transaction_config& config()
        {
            return config_;
        }

        /**
         * @internal
         * Called internally
         */
        CB_NODISCARD const transactions_cleanup& cleanup() const
        {
            return *cleanup_;
        }

        /**
         * @internal
         * Called internally
         */
        CB_NODISCARD transactions_cleanup& cleanup()
        {
            return *cleanup_;
        }

        /**
         * @brief Return a reference to the @ref cluster
         *
         * @return Ref to the cluster used by this transaction object.
         */
        CB_NODISCARD couchbase::cluster& cluster_ref()
        {
            return cluster_;
        }

      private:
        couchbase::cluster& cluster_;
        transaction_config config_;
        std::unique_ptr<transactions_cleanup> cleanup_;
        const int max_attempts_{ 10 };
        const std::chrono::milliseconds min_retry_delay_{ 10 };
    };
} // namespace transactions
} // namespace couchbase
