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
#include "couchbase/transactions/internal/exceptions_internal.hxx"
#include "couchbase/transactions/internal/logging.hxx"
#include "couchbase/transactions/internal/transaction_context.hxx"
#include "couchbase/transactions/internal/transactions_cleanup.hxx"
#include "couchbase/transactions/internal/utils.hxx"
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
wrap_run(tx::transactions& txns, const tx::per_transaction_config& config, size_t max_attempts, Handler&& fn)
{
    tx::transaction_context overall(txns, config);
    size_t attempts{ 0 };
    while (attempts++ < max_attempts) {
        // NOTE: new_attempt_context has the exponential backoff built in.  So, after
        // the first time it is called, it has a 1ms delay, then 2ms, etc... capped at 100ms
        // until (for now) a timeout is reached (2x the expiration_time).   Soon, will build in
        // a max attempts instead.  In any case, the timeout occurs in the logic - adding
        // a max attempts or timeout is just in case a bug prevents timeout, etc...
        overall.new_attempt_context();
        auto barrier = std::make_shared<std::promise<std::optional<tx::transaction_result>>>();
        auto f = barrier->get_future();
        auto finalize_handler = [&, barrier](std::optional<tx::transaction_exception> err, std::optional<tx::transaction_result> result) {
            if (result) {
                return barrier->set_value(result);
            } else if (err) {
                return barrier->set_exception(std::make_exception_ptr(*err));
            }
            barrier->set_value({});
        };
        try {
            auto ctx = overall.current_attempt_context();
            fn(*ctx);
        } catch (...) {
            overall.handle_error(std::current_exception(), finalize_handler);
            auto retval = f.get();
            if (retval) {
                // no return value, no exception means retry.
                return *retval;
            }
            continue;
        }
        overall.finalize(finalize_handler);
        auto retval = f.get();
        if (retval) {
            return *retval;
        }
        continue;
    }
    // only thing to do here is return, but we really exceeded the max attempts
    assert(true);
    return overall.get_transaction_result();
}

tx::transaction_result
tx::transactions::run(logic&& logic)
{
    per_transaction_config config;
    return wrap_run(*this, config, max_attempts_, std::move(logic));
}

tx::transaction_result
tx::transactions::run(const per_transaction_config& config, logic&& logic)
{
    return wrap_run(*this, config, max_attempts_, std::move(logic));
}

void
tx::transactions::run(const per_transaction_config& config, async_logic&& logic, txn_complete_callback&& cb)
{
    std::thread([this, config, logic = std::move(logic), cb = std::move(cb)] {
        try {
            auto result = wrap_run(*this, config, max_attempts_, std::move(logic));
            return cb({}, result);
        } catch (const transaction_exception& e) {
            return cb(e, std::nullopt);
        }
    })
      .detach();
}
void
tx::transactions::run(async_logic&& logic, txn_complete_callback&& cb)
{
    per_transaction_config config;
    return run(config, std::move(logic), std::move(cb));
}

void
tx::transactions::close()
{
    txn_log->info("closing transactions");
    cleanup_->close();
    txn_log->info("transactions closed");
}
