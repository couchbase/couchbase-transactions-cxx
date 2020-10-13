/*
 *     Copyright 2020 Couchbase, Inc.
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

#include <boost/optional.hpp>

#include <couchbase/client/collection.hxx>
#include <couchbase/transactions/atr_entry.hxx>
#include <couchbase/transactions/exceptions.hxx>
#include <couchbase/transactions/transaction_config.hxx>
#include <utility>

namespace couchbase
{
namespace transactions
{
    class active_transaction_record
    {
      public:
        static boost::optional<active_transaction_record> get_atr(std::shared_ptr<collection> collection,
                                                                  const std::string& atr_id);

        active_transaction_record(std::string id,
                                  std::shared_ptr<collection> collection,
                                  uint64_t cas,
                                  std::vector<atr_entry> entries)
          : id_(std::move(id))
          , collection_(collection)
          , cas_ns_(cas)
          , entries_(std::move(entries))
        {
        }

        CB_NODISCARD const std::vector<atr_entry>& entries() const
        {
            return entries_;
        }

      private:
        const std::string id_;
        std::shared_ptr<collection> collection_;
        const uint64_t cas_ns_;
        const std::vector<atr_entry> entries_;

        static inline uint64_t parse_mutation_cas(const std::string& cas);
        static inline boost::optional<std::vector<doc_record>> process_document_ids(nlohmann::json& entry, std::string key);
        static inline active_transaction_record map_to_atr(std::shared_ptr<collection> collection,
                                                           const std::string& atr_id,
                                                           result& res,
                                                           nlohmann::json& attempts);
    };

} // namespace transactions
} // namespace couchbase
