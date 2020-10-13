#include <couchbase/transactions.hxx>


namespace tx = couchbase::transactions;

tx::transactions::transactions(couchbase::cluster& cluster, const transaction_config& config)
    : cluster_(cluster)
    , config_(config)
    , cleanup_(cluster_, config_)
{
    spdlog::info("couchbase transactions {} creating new transaction object", VERSION_STR);
}

std::shared_ptr<tx::transactions>
tx::transactions::clone(couchbase::cluster& new_cluster,
                        std::shared_ptr<tx::attempt_context_testing_hooks> new_hooks,
                        std::shared_ptr<tx::cleanup_testing_hooks> new_cleanup_hooks)
{
    spdlog::info("couchbase transactions {} copying transaction object", VERSION_STR);
    // TODO - actually copy the resto of config
    tx::transaction_config config = config_;
    config.test_factories(*new_hooks, *new_cleanup_hooks);
    return std::make_shared<transactions>(new_cluster, config);
}

tx::transaction_result
tx::transactions::run(const logic& logic)
{
    tx::transaction_context overall;
    while (overall.num_attempts() < max_attempts_) {
        tx::attempt_context ctx(this, overall, config_);
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
    return tx::transaction_result{ overall.transaction_id(),
        overall.atr_id(),
        overall.atr_collection(),
        overall.attempts(),
        overall.current_attempt().state == attempt_state::COMPLETED };
}

void
tx::transactions::close()
{
    spdlog::info("closing transactions");
    cleanup_.close();
    spdlog::info("transactions closed");
}
