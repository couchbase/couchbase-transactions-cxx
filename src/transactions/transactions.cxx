/*
 *     Copyright 2021 Couchbase, Inc.
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

#include "attempt_context_impl.hxx"
#include "couchbase/transactions/internal/logging.hxx"
#include "couchbase/transactions/internal/transaction_context.hxx"
#include "couchbase/transactions/internal/transactions_cleanup.hxx"
#include "exceptions_internal.hxx"
#include "utils.hxx"
#include <couchbase/transactions.hxx>

namespace tx = couchbase::transactions;

tx::transactions::transactions(cluster& cluster, const transaction_config& config)
  : cluster_(cluster)
  , config_(config)
  , cleanup_(new transactions_cleanup(cluster_, config_))
{
    txn_log->info("couchbase transactions {}{} creating new transaction object", VERSION_STR, VERSION_SHA);
}

tx::transactions::~transactions() = default;

template<typename Handler>
tx::transaction_result
wrap_run(tx::transactions& txns, Handler&& fn)
{
    tx::transaction_context overall(txns);
    // This will exponentially backoff, doubling the retry delay each time until the 8th time, and cap it at
    // 128*<initial retry delay>, and cap this at the max retries.  NOTE: this sets a maximum effective
    // duration of the transaction of something like max_retries_ * 128 * min_retry_delay.  We could do better
    // by making it exponential over the duration, at some point.
    return tx::retry_op_exponential_backoff<tx::transaction_result>(std::chrono::milliseconds(1), 1000, [&] {
        tx::attempt_context_impl ctx(overall);
        tx::txn_log->info("starting attempt {}/{}/{}", overall.num_attempts(), overall.transaction_id(), ctx.id());
        try {
            fn(ctx);
            ctx.existing_error(false);
            if (!ctx.is_done()) {
                ctx.commit();
            }
            overall.cleanup().add_attempt(ctx);
        } catch (const tx::transaction_operation_failed& er) {
            tx::txn_log->error("got transaction_operation_failed {}", er.what());
            if (er.should_rollback()) {
                tx::txn_log->trace("got rollback-able exception, rolling back");
                try {
                    ctx.rollback();
                } catch (const std::runtime_error& er_rollback) {
                    overall.cleanup().add_attempt(ctx);
                    tx::txn_log->trace("got error {} while auto rolling back, throwing original error", er_rollback.what(), er.what());
                    er.do_throw(overall);
                    // if you get here, we didn't throw, yet we had an error.  Fall through in
                    // this case.  Note the current logic is such that rollback will not have a
                    // commit ambiguous error, so we should always throw.
                    assert(true);
                }
                if (er.should_retry() && overall.has_expired_client_side(overall.config())) {
                    tx::txn_log->trace("auto rollback succeeded, however we are expired so no retry");
                    // this always throws
                    tx::transaction_operation_failed(tx::FAIL_EXPIRY, "expired in auto rollback").no_rollback().expired().do_throw(overall);
                }
            }
            if (er.should_retry()) {
                tx::txn_log->trace("got retryable exception, retrying");
                overall.cleanup().add_attempt(ctx);
                throw tx::retry_operation("retry transaction");
            }

            // throw the expected exception here
            overall.cleanup().add_attempt(ctx);
            er.do_throw(overall);
            // if we don't throw, we will fall through and return.
        } catch (const std::exception& ex) {
            tx::txn_log->error("got runtime error {}", ex.what());
            try {
                ctx.rollback();
            } catch (...) {
                tx::txn_log->error("got error rolling back {}", ex.what());
            }
            overall.cleanup().add_attempt(ctx);
            // the assumption here is this must come from the logic, not
            // our operations (which only throw transaction_operation_failed),
            auto op_failed = tx::transaction_operation_failed(tx::FAIL_OTHER, ex.what());
            op_failed.do_throw(overall);
        } catch (...) {
            tx::txn_log->error("got unexpected error, rolling back");
            try {
                ctx.rollback();
            } catch (...) {
                tx::txn_log->error("got error rolling back unexpected error");
            }
            overall.cleanup().add_attempt(ctx);
            // the assumption here is this must come from the logic, not
            // our operations (which only throw transaction_operation_failed),
            auto op_failed = tx::transaction_operation_failed(tx::FAIL_OTHER, "Unexpected error");
            op_failed.do_throw(overall);
        }
        return overall.get_transaction_result();
    });
}

tx::transaction_result
tx::transactions::run(logic&& logic)
{
    return wrap_run(*this, logic);
}

void
tx::transactions::run(async_logic&& logic, txn_complete_callback&& cb)
{
    std::async(std::launch::async, [this, logic = std::move(logic), cb = std::move(cb)] {
        try {
            auto result = wrap_run(*this, logic);
            return cb({}, result);
        } catch (const transaction_exception& e) {
            return cb(e, std::nullopt);
        }
    });
}

void
tx::transactions::close()
{
    txn_log->info("closing transactions");
    cleanup_->close();
    txn_log->info("transactions closed");
}
