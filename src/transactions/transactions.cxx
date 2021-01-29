#include "attempt_context_impl.hxx"
#include "exceptions_internal.hxx"
#include "logging.hxx"
#include "transactions_cleanup.hxx"
#include <couchbase/transactions.hxx>

namespace tx = couchbase::transactions;

tx::transactions::transactions(couchbase::cluster& cluster, const transaction_config& config)
  : cluster_(cluster)
  , config_(config)
  , cleanup_(new transactions_cleanup(cluster_, config_))
{
    txn_log->info("couchbase transactions {} creating new transaction object", VERSION_STR);
}

tx::transactions::~transactions() = default;

tx::transaction_result
tx::transactions::run(const logic& logic)
{
    tx::transaction_context overall;
    while (overall.num_attempts() < max_attempts_) {
        tx::attempt_context_impl ctx(this, overall, config_);
        txn_log->info("starting attempt {}/{}/{}", overall.num_attempts(), overall.transaction_id(), ctx.id());
        try {
            logic(ctx);
            if (!ctx.is_done()) {
                ctx.commit();
            }
            cleanup_->add_attempt(ctx);
            break;
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
                    // if you get here, we didn't throw, yet we had an error
                    // probably should stop retries, though need to check this
                    assert(true || "should never reach this");
                    break;
                }
                if (er.should_retry() && overall.has_expired_client_side(config_)) {
                    txn_log->trace("auto rollback succeeded, however we are expired so no retry");
                    // this always throws
                    transaction_operation_failed(FAIL_EXPIRY, "expired in auto rollback").no_rollback().expired().do_throw(overall);
                }
            }
            if (er.should_retry()) {
                if (overall.num_attempts() < max_attempts_) {
                    txn_log->trace("got retryable exception, retrying");

                    // simple linear backoff with #of attempts
                    std::this_thread::sleep_for(min_retry_delay_ * pow(2, fmin(10, overall.num_attempts())));
                    cleanup_->add_attempt(ctx);
                    continue;
                }
            }

            // throw the expected exception here
            cleanup_->add_attempt(ctx);
            er.do_throw(overall);
            // if we don't throw, break here means no retry
            break;
        } catch (const std::exception& ex) {
            txn_log->error("got runtime error {}", ex.what());
            ctx.rollback();
            cleanup_->add_attempt(ctx);
            break;
        } catch (...) {
            txn_log->error("got unexpected error, rolling back");
            ctx.rollback();
            cleanup_->add_attempt(ctx);
            break;
        }
    }
    return overall.get_transaction_result();
}

void
tx::transactions::close()
{
    txn_log->info("closing transactions");
    cleanup_->close();
    txn_log->info("transactions closed");
}
