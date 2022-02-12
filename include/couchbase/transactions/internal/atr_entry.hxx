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

#pragma once

#include <chrono>
#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include <optional>

#include <couchbase/support.hxx>
#include <couchbase/transactions/attempt_state.hxx>

#include "doc_record.hxx"

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
                  std::optional<std::uint64_t> timestamp_start_ms,
                  std::optional<std::uint64_t> timestamp_commit_ms,
                  std::optional<std::uint64_t> timestamp_complete_ms,
                  std::optional<std::uint64_t> timestamp_rollback_ms,
                  std::optional<std::uint64_t> timestamp_rolled_back_ms,
                  std::optional<std::uint32_t> expires_after_ms,
                  std::optional<std::vector<doc_record>> inserted_ids,
                  std::optional<std::vector<doc_record>> replaced_ids,
                  std::optional<std::vector<doc_record>> removed_ids,
                  std::optional<nlohmann::json> forward_compat,
                  std::uint64_t cas,
                  std::optional<std::string> durability_level)
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
          , forward_compat_(std::move(forward_compat))
          , cas_(cas)
          , durability_level_(durability_level)
        {
        }

        CB_NODISCARD bool has_expired(std::uint32_t safety_margin = 0) const
        {
            uint64_t cas_ms = cas_ / 1000000;
            if (timestamp_start_ms_ && cas_ms > *timestamp_start_ms_) {
                uint32_t expires_after_ms = *expires_after_ms_;
                return (cas_ms - *timestamp_start_ms_) > (expires_after_ms + safety_margin);
            }
            return false;
        }

        CB_NODISCARD std::uint32_t age_ms() const
        {
            return static_cast<uint32_t>((cas_ / 1000000) - timestamp_start_ms_.value_or(0));
        }

        CB_NODISCARD const std::string& atr_id() const
        {
            return atr_id_;
        }

        CB_NODISCARD const std::string& attempt_id() const
        {
            return attempt_id_;
        }

        CB_NODISCARD std::optional<std::uint64_t> timestamp_start_ms() const
        {
            return timestamp_start_ms_;
        }
        CB_NODISCARD std::optional<std::uint64_t> timestamp_commit_ms() const
        {
            return timestamp_commit_ms_;
        }
        CB_NODISCARD std::optional<std::uint64_t> timestamp_complete_ms() const
        {
            return timestamp_complete_ms_;
        }
        CB_NODISCARD std::optional<std::uint64_t> timestamp_rollback_ms() const
        {
            return timestamp_rollback_ms_;
        }
        CB_NODISCARD std::optional<std::uint64_t> timestamp_rolled_back_ms() const
        {
            return timestamp_rolled_back_ms_;
        }

        /**
         * Returns the CAS of the ATR document containing this entry
         */
        CB_NODISCARD std::uint64_t cas() const
        {
            return cas_;
        }

        CB_NODISCARD std::optional<std::vector<doc_record>> inserted_ids() const
        {
            return inserted_ids_;
        }

        CB_NODISCARD std::optional<std::vector<doc_record>> replaced_ids() const
        {
            return replaced_ids_;
        }

        CB_NODISCARD std::optional<std::vector<doc_record>> removed_ids() const
        {
            return removed_ids_;
        }

        CB_NODISCARD std::optional<nlohmann::json> forward_compat() const
        {
            return forward_compat_;
        }

        CB_NODISCARD std::optional<std::uint32_t> expires_after_ms() const
        {
            return expires_after_ms_;
        }

        CB_NODISCARD attempt_state state() const
        {
            return state_;
        }

        CB_NODISCARD std::optional<std::string> durability_level() const
        {
            return durability_level_;
        }

      private:
        std::string atr_bucket_;
        std::string atr_id_;
        std::string attempt_id_;
        attempt_state state_ = attempt_state::NOT_STARTED;
        std::optional<std::uint64_t> timestamp_start_ms_;
        std::optional<std::uint64_t> timestamp_commit_ms_;
        std::optional<std::uint64_t> timestamp_complete_ms_;
        std::optional<std::uint64_t> timestamp_rollback_ms_;
        std::optional<std::uint64_t> timestamp_rolled_back_ms_;
        std::optional<std::uint32_t> expires_after_ms_;
        std::optional<std::vector<doc_record>> inserted_ids_;
        std::optional<std::vector<doc_record>> replaced_ids_;
        std::optional<std::vector<doc_record>> removed_ids_;
        std::optional<nlohmann::json> forward_compat_;
        std::uint64_t cas_{};
        // ExtStoreDurability
        std::optional<std::string> durability_level_;
    };
} // namespace transactions
} // namespace couchbase
