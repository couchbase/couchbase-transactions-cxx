#pragma once

#include <chrono>
#include <string>
#include <vector>

#include <couchbase/transactions/transaction_attempt.hxx>
#include <couchbase/transactions/uid_generator.hxx>

namespace couchbase
{
namespace transactions
{
    class transaction_context
    {
      public:
        transaction_context()
          : transaction_id_(uid_generator::next())
          , start_time_client_(std::chrono::system_clock::now())
          , deferred_elapsed_(0)
        {
        }

        [[nodiscard]] const std::string& transaction_id() const
        {
            return transaction_id_;
        }

        [[nodiscard]] size_t num_attempts() const
        {
            return attempts_.size();
        }

        [[nodiscard]] bool has_expired_client_side(const transaction_config& config)
        {
            const auto& now = std::chrono::system_clock::now();
            auto expired_nanos = std::chrono::duration_cast<std::chrono::nanoseconds>(now - start_time_client_) + deferred_elapsed_;
            auto expired_millis = std::chrono::duration_cast<std::chrono::milliseconds>(expired_nanos);
            bool is_expired = expired_nanos > config.transaction_expiration_time();
            if (is_expired) {
                spdlog::info("has expired client side (now={}ns, start={}ns, deferred_elapsed={}ns, expired={}ns ({}ms), config={}ms)",
                             now.time_since_epoch().count(),
                             start_time_client_.time_since_epoch().count(),
                             deferred_elapsed_.count(),
                             expired_nanos.count(),
                             expired_millis.count(),
                             std::chrono::duration_cast<std::chrono::milliseconds>(config.transaction_expiration_time()).count());
            }
            return is_expired;
        }

        [[nodiscard]] std::chrono::time_point<std::chrono::system_clock> start_time_client() const
        {
            return start_time_client_;
        }

      private:
        std::string transaction_id_;

        /** The time this overall transaction started */
        const std::chrono::time_point<std::chrono::system_clock> start_time_client_;

        /**
         * Will be non-zero only when resuming a deferred transaction. It records how much time has elapsed in total in the deferred
         * transaction, including the time spent in the original transaction plus any time spent while deferred.
         */
        const std::chrono::nanoseconds deferred_elapsed_;

        std::vector<transaction_attempt> attempts_;
    };
} // namespace transactions
} // namespace couchbase
