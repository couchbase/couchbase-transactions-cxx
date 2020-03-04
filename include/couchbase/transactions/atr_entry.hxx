#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <boost/optional.hpp>

#include <couchbase/transactions/attempt_state.hxx>
#include <couchbase/transactions/doc_record.hxx>

namespace couchbase
{
namespace transactions
{
    struct atr_entry {
      public:
        atr_entry() = default;
        atr_entry(std::string atr_bucket,
                  std::string atr_id,
                  std::string attempt_id,
                  attempt_state state,
                  boost::optional<std::uint32_t> timestamp_start_ms,
                  boost::optional<std::uint32_t> timestamp_commit_ms,
                  boost::optional<std::uint32_t> timestamp_complete_ms,
                  boost::optional<std::uint32_t> timestamp_rollback_ms,
                  boost::optional<std::uint32_t> timestamp_rolled_back_ms,
                  boost::optional<std::uint32_t> expires_after_ms,
                  boost::optional<std::vector<doc_record>> inserted_ids,
                  boost::optional<std::vector<doc_record>> replaced_ids,
                  boost::optional<std::vector<doc_record>> removed_ids,
                  std::uint64_t cas)
          : atr_bucket_(std::move(atr_bucket))
          , atr_id_(std::move(atr_id))
          , attempt_id_(std::move(attempt_id))
          , state_(state)
          , timestamp_start_ms_(timestamp_start_ms)
          , timestamp_commit_ms_(timestamp_commit_ms)
          , timestamp_complete_ms_(timestamp_complete_ms)
          , timestamp_rollback_ms_(timestamp_rollback_ms)
          , timestamp_rolled_back_ms_(timestamp_rolled_back_ms)
          , expires_after_ms_(expires_after_ms)
          , inserted_ids_(std::move(inserted_ids))
          , replaced_ids_(std::move(replaced_ids))
          , removed_ids_(std::move(removed_ids))
          , cas_(cas)
        {
        }

        [[nodiscard]] bool has_expired(std::uint32_t safety_margin = 0) const
        {
            std::uint32_t cas_ms = cas_ / 1000000;
            if (expires_after_ms_.has_value()) {
                std::uint32_t expires_after_ms = *expires_after_ms_;
                return (cas_ms - *expires_after_ms_) > expires_after_ms + safety_margin;
            }
            return false;
        }

        [[nodiscard]] std::uint32_t age_ms() const
        {
            return (cas_ / 1000000) - timestamp_start_ms_.value_or(0);
        }

        [[nodiscard]] const std::string& atr_id() const
        {
            return atr_id_;
        }

        [[nodiscard]] const std::string& attempt_id() const
        {
            return attempt_id_;
        }

        [[nodiscard]] boost::optional<std::uint32_t> timestamp_start_ms() const
        {
            return timestamp_start_ms_;
        }
        [[nodiscard]] boost::optional<std::uint32_t> timestamp_commit_ms() const
        {
            return timestamp_commit_ms_;
        }
        [[nodiscard]] boost::optional<std::uint32_t> timestamp_complete_ms() const
        {
            return timestamp_complete_ms_;
        }
        [[nodiscard]] boost::optional<std::uint32_t> timestamp_rollback_ms() const
        {
            return timestamp_rollback_ms_;
        }
        [[nodiscard]] boost::optional<std::uint32_t> timestamp_rolled_back_ms() const
        {
            return timestamp_rolled_back_ms_;
        }

        /**
         * Returns the CAS of the ATR document containing this entry
         */
        [[nodiscard]] std::uint64_t cas() const
        {
            return cas_;
        }

        [[nodiscard]] boost::optional<std::vector<doc_record>> inserted_ids() const
        {
            return inserted_ids_;
        }

        [[nodiscard]] boost::optional<std::vector<doc_record>> replaced_ids() const
        {
            return replaced_ids_;
        }

        [[nodiscard]] boost::optional<std::vector<doc_record>> removed_ids() const
        {
            return removed_ids_;
        }

        [[nodiscard]] boost::optional<std::uint32_t> expires_after_ms() const
        {
            return expires_after_ms_;
        }

        [[nodiscard]] attempt_state state() const
        {
            return state_;
        }

      private:
        const std::string atr_bucket_;
        const std::string atr_id_;
        const std::string attempt_id_;
        const attempt_state state_ = attempt_state::NOT_STARTED;
        const boost::optional<std::uint32_t> timestamp_start_ms_;
        const boost::optional<std::uint32_t> timestamp_commit_ms_;
        const boost::optional<std::uint32_t> timestamp_complete_ms_;
        const boost::optional<std::uint32_t> timestamp_rollback_ms_;
        const boost::optional<std::uint32_t> timestamp_rolled_back_ms_;
        const boost::optional<std::uint32_t> expires_after_ms_;
        const boost::optional<std::vector<doc_record>> inserted_ids_;
        const boost::optional<std::vector<doc_record>> replaced_ids_;
        const boost::optional<std::vector<doc_record>> removed_ids_;
        const std::uint64_t cas_{};
    };
} // namespace transactions
} // namespace couchbase
