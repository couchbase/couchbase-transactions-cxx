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

#include <cstdint>
#include <string>
#include <utility>

#include <optional>

#include <couchbase/cluster.hxx>
#include <couchbase/transactions/exceptions.hxx>
#include <couchbase/transactions/result.hxx>
#include <couchbase/transactions/transaction_config.hxx>

#include "atr_entry.hxx"

namespace couchbase
{
namespace transactions
{
    class active_transaction_record
    {
      public:
        static std::optional<active_transaction_record> get_atr(cluster& cluster, const couchbase::document_id& atr_id);

        active_transaction_record(const couchbase::document_id& id, uint64_t, std::vector<atr_entry> entries)
          : id_(std::move(id))
          , entries_(std::move(entries))
        {
        }

        CB_NODISCARD const std::vector<atr_entry>& entries() const
        {
            return entries_;
        }

      private:
        const couchbase::document_id id_;
        const std::vector<atr_entry> entries_;

        static inline uint64_t parse_mutation_cas(const std::string& cas);
        static inline std::optional<std::vector<doc_record>> process_document_ids(nlohmann::json& entry, std::string key);
        static inline active_transaction_record map_to_atr(const couchbase::document_id& atr_id, result& res, nlohmann::json& attempts);
    };

} // namespace transactions
} // namespace couchbase
