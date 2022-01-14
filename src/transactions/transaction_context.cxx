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
#include "uid_generator.hxx"

#include <couchbase/transactions/internal/logging.hxx>
#include <couchbase/transactions/internal/transaction_context.hxx>

namespace couchbase
{
namespace transactions
{
    transaction_context::transaction_context(transactions& txns, const per_transaction_config& config)
      : transaction_id_(uid_generator::next())
      , transactions_(txns)
      , config_(config.apply(txns.config()))
      , start_time_client_(std::chrono::steady_clock::now())
      , deferred_elapsed_(0)
      , cleanup_(txns.cleanup())
    {
    }

    void transaction_context::add_attempt()
    {
        transaction_attempt attempt{};
        attempts_.push_back(attempt);
    }

    CB_NODISCARD std::chrono::nanoseconds transaction_context::remaining() const
    {
        const auto& now = std::chrono::steady_clock::now();
        auto expired_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_time_client_) + deferred_elapsed_;
        return config_.expiration_time() - expired_nanos;
    }

    CB_NODISCARD bool transaction_context::has_expired_client_side()
    {
        // repeat code above - nice for logging.  Ponder changing this.
        const auto& now = std::chrono::steady_clock::now();
        auto expired_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_time_client_) + deferred_elapsed_;
        auto expired_millis = std::chrono::duration_cast<std::chrono::milliseconds>(expired_nanos);
        bool is_expired = expired_nanos > config_.expiration_time();
        if (is_expired) {
            txn_log->info("has expired client side (now={}ns, start={}ns, deferred_elapsed={}ns, expired={}ns ({}ms), config={}ms)",
                          now.time_since_epoch().count(),
                          start_time_client_.time_since_epoch().count(),
                          deferred_elapsed_.count(),
                          expired_nanos.count(),
                          expired_millis.count(),
                          std::chrono::duration_cast<std::chrono::milliseconds>(config_.expiration_time()).count());
        }
        return is_expired;
    }

    void transaction_context::retry_delay()
    {
        // when we retry an operation, we typically call that function recursively.  So, we need to
        // limit total number of times we do it.  Later we can be more sophisticated, perhaps.
        auto delay = config_.expiration_time() / 100; // the 100 is arbitrary
        txn_log->trace("about to sleep for {} ms", std::chrono::duration_cast<std::chrono::milliseconds>(delay).count());
        std::this_thread::sleep_for(delay);
    }

    void transaction_context::new_attempt_context()
    {
        current_attempt_context_ = std::make_shared<attempt_context_impl>(*this);
    }

    std::shared_ptr<attempt_context_impl> transaction_context::current_attempt_context()
    {
        return current_attempt_context_;
    }

    void transaction_context::get(const couchbase::document_id& id, async_attempt_context::Callback&& cb)
    {
        if (current_attempt_context_) {
            return current_attempt_context_->get(id, std::move(cb));
        }
        throw transaction_operation_failed(FAIL_OTHER, "no current attempt context");
    }

    void transaction_context::get_optional(const couchbase::document_id& id, async_attempt_context::Callback&& cb)
    {
        if (current_attempt_context_) {
            return current_attempt_context_->get_optional(id, std::move(cb));
        }
        throw transaction_operation_failed(FAIL_OTHER, "no current attempt context");
    }

    void transaction_context::insert(const couchbase::document_id& id, const std::string& content, async_attempt_context::Callback&& cb)
    {
        if (current_attempt_context_) {
            return current_attempt_context_->insert_raw(id, content, std::move(cb));
        }
        throw transaction_operation_failed(FAIL_OTHER, "no current attempt context");
    }

    void transaction_context::replace(const transaction_get_result& doc, const std::string& content, async_attempt_context::Callback&& cb)
    {
        if (current_attempt_context_) {
            return current_attempt_context_->replace_raw(doc, content, std::move(cb));
        }
        throw transaction_operation_failed(FAIL_OTHER, "no current attempt context");
    }

    void transaction_context::remove(const transaction_get_result& doc, async_attempt_context::VoidCallback&& cb)
    {
        if (current_attempt_context_) {
            return current_attempt_context_->remove(doc, std::move(cb));
        }
        throw transaction_operation_failed(FAIL_OTHER, "no current attempt context");
    }

    void transaction_context::query(const std::string& statement,
                                    const transaction_query_options& opts,
                                    async_attempt_context::QueryCallback&& cb)
    {
        if (current_attempt_context_) {
            return current_attempt_context_->query(statement, opts, std::move(cb));
        }
        throw(transaction_operation_failed(FAIL_OTHER, "no current attempt context"));
    }

    void transaction_context::commit(async_attempt_context::VoidCallback&& cb)
    {
        if (current_attempt_context_) {
            return current_attempt_context_->commit(std::move(cb));
        }
        throw transaction_operation_failed(FAIL_OTHER, "no current attempt context").no_rollback();
    }

    void transaction_context::rollback(async_attempt_context::VoidCallback&& cb)
    {
        if (current_attempt_context_) {
            return current_attempt_context_->rollback(std::move(cb));
        }
        throw transaction_operation_failed(FAIL_OTHER, "no current attempt context").no_rollback();
    }

    void transaction_context::existing_error()
    {
        if (current_attempt_context_) {
            return current_attempt_context_->existing_error();
        }
        throw transaction_operation_failed(FAIL_OTHER, "no current attempt context").no_rollback();
    }

} // namespace transactions
} // namespace couchbase
