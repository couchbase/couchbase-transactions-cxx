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
#include "exceptions_internal.hxx"
#include "logging.hxx"
#include "transactions_cleanup.hxx"
#include "utils.hxx"

#include <couchbase/transactions.hxx>

namespace tx = couchbase::transactions;

tx::transactions::transactions(couchbase::cluster& cluster, const transaction_config& config)
  : cluster_(cluster)
  , config_(config)
  , cleanup_(new transactions_cleanup(cluster_, config_))
{
    txn_log->info("couchbase transactions {}{} creating new transaction object", VERSION_STR, VERSION_SHA);
}

tx::transactions::~transactions() = default;

tx::transaction_result
tx::transactions::run(const logic& logic)
{
    tx::transaction_context overall;
    // This will exponentially backoff, doubling the retry delay each time until the 8th time, and cap it at
    // 128*<initial retry delay>, and cap this at the max retries.  NOTE: this sets a maximum effective
    // duration of the transaction of something like max_retries_ * 128 * min_retry_delay.  We could do better
    // by making it exponential over the duration, at some point.
    return retry_op_exponential_backoff<tx::transaction_result>(min_retry_delay_, max_attempts_, [&] {
        tx::attempt_context_impl ctx(this, overall, config_);
        txn_log->info("starting attempt {}/{}/{}", overall.num_attempts(), overall.transaction_id(), ctx.id());
        try {
            logic(ctx);
            if (!ctx.is_done()) {
                ctx.commit();
            }
            cleanup_->add_attempt(ctx);
        } catch (const transaction_operation_failed& er) {
            txn_log->error("got transaction_operation_failed {}", er.what());
            if (er.should_rollback()) {
                txn_log->trace("got rollback-able exception, rolling back");
                try {
                    ctx.rollback();
                } catch (const std::runtime_error& er_rollback) {
                    cleanup_->add_attempt(ctx);
                    txn_log->trace("got error {} while auto rolling back, throwing original error", er_rollback.what(), er.what());
                    er.do_throw(overall);
                    // if you get here, we didn't throw, yet we had an error.  Fall through in
                    // this case.  Note the current logic is such that rollback will not have a
                    // commit ambiguous error, so we should always throw.
                    assert(true || "should never reach this");
                }
                if (er.should_retry() && overall.has_expired_client_side(config_)) {
                    txn_log->trace("auto rollback succeeded, however we are expired so no retry");
                    // this always throws
                    transaction_operation_failed(FAIL_EXPIRY, "expired in auto rollback").no_rollback().expired().do_throw(overall);
                }
            }
            if (er.should_retry()) {
                txn_log->trace("got retryable exception, retrying");
                cleanup_->add_attempt(ctx);
                throw tx::retry_operation("retry transaction");
            }

            // throw the expected exception here
            cleanup_->add_attempt(ctx);
            er.do_throw(overall);
            // if we don't throw, we will fall through and return.
        } catch (const std::exception& ex) {
            txn_log->error("got runtime error {}", ex.what());
            try {
                ctx.rollback();
            } catch (...) {
                txn_log->error("got error rolling back {}", ex.what());
            }
            cleanup_->add_attempt(ctx);
            // the assumption here is this must come from the logic, not
            // our operations (which only throw transaction_operation_failed),
            auto op_failed = transaction_operation_failed(FAIL_OTHER, ex.what());
            op_failed.do_throw(overall);
        } catch (...) {
            txn_log->error("got unexpected error, rolling back");
            try {
                ctx.rollback();
            } catch (...) {
                txn_log->error("got error rolling back unexpected error");
            }
            cleanup_->add_attempt(ctx);
            // the assumption here is this must come from the logic, not
            // our operations (which only throw transaction_operation_failed),
            auto op_failed = transaction_operation_failed(FAIL_OTHER, "Unexpected error");
            op_failed.do_throw(overall);
        }
        return overall.get_transaction_result();
    });
}

void
tx::transactions::close()
{
    txn_log->info("closing transactions");
    cleanup_->close();
    txn_log->info("transactions closed");
}
