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

#include <libcouchbase/couchbase.h>

#include "active_transaction_record.hxx"
#include "exceptions_internal.hxx"
#include "utils.hxx"

namespace couchbase
{
namespace transactions
{

    /**
     * ${Mutation.CAS} is written by kvengine with 'macroToString(htonll(info.cas))'.  Discussed this with KV team and, though there is
     * consensus that this is off (htonll is definitely wrong, and a string is an odd choice), there are clients (SyncGateway) that
     * consume the current string, so it can't be changed.  Note that only little-endian servers are supported for Couchbase, so the 8
     * byte long inside the string will always be little-endian ordered.
     *
     * Looks like: "0x000058a71dd25c15"
     * Want:        0x155CD21DA7580000   (1539336197457313792 in base10, an epoch time in millionths of a second)
     *
     * returns epoch time in ms
     */
    inline uint64_t active_transaction_record::parse_mutation_cas(const std::string& cas)
    {
        if (cas.empty()) {
            return 0;
        }

        uint64_t val = stoull(cas, nullptr, 16);
        /* byteswap */
        size_t ii;
        uint64_t ret = 0;
        for (ii = 0; ii < sizeof(uint64_t); ii++) {
            ret <<= 8ull;
            ret |= val & 0xffull;
            val >>= 8ull;
        }
        return ret / 1000000;
    }

    inline boost::optional<std::vector<doc_record>> active_transaction_record::process_document_ids(nlohmann::json& entry, std::string key)
    {
        if (entry.count(key) == 0) {
            return {};
        }
        std::vector<doc_record> records;
        records.reserve(entry[key].size());
        for (auto& record : entry[key]) {
            records.push_back(doc_record::create_from(record));
        }
        return std::move(records);
    }

    inline active_transaction_record active_transaction_record::map_to_atr(std::shared_ptr<collection> collection,
                                                                           const std::string& atr_id,
                                                                           result& res,
                                                                           nlohmann::json& attempts)
    {
        auto vbucket = res.values[1].content_as<nlohmann::json>();
        auto now_ns = now_ns_from_vbucket(vbucket);
        std::vector<atr_entry> entries;
        entries.reserve(attempts.size());
        for (auto& element : attempts.items()) {
            auto& val = element.value();
            entries.emplace_back(
              collection->bucket_name(),
              atr_id,
              element.key(),
              attempt_state_value(val[ATR_FIELD_STATUS].get<std::string>()),
              parse_mutation_cas(val.value(ATR_FIELD_START_TIMESTAMP, "")),
              parse_mutation_cas(val.value(ATR_FIELD_START_COMMIT, "")),
              parse_mutation_cas(val.value(ATR_FIELD_TIMESTAMP_COMPLETE, "")),
              parse_mutation_cas(val.value(ATR_FIELD_TIMESTAMP_ROLLBACK_START, "")),
              parse_mutation_cas(val.value(ATR_FIELD_TIMESTAMP_ROLLBACK_COMPLETE, "")),
              val.count(ATR_FIELD_EXPIRES_AFTER_MSECS) ? boost::make_optional(val[ATR_FIELD_EXPIRES_AFTER_MSECS].get<std::uint32_t>())
                                                       : boost::optional<std::uint32_t>(),
              process_document_ids(val, ATR_FIELD_DOCS_INSERTED),
              process_document_ids(val, ATR_FIELD_DOCS_REPLACED),
              process_document_ids(val, ATR_FIELD_DOCS_REMOVED),
              val.contains(ATR_FIELD_FORWARD_COMPAT) ? boost::make_optional(val[ATR_FIELD_FORWARD_COMPAT].get<nlohmann::json>())
                                                     : boost::none,
              now_ns);
        }
        return active_transaction_record(atr_id, collection, res.cas, std::move(entries));
    }

    boost::optional<active_transaction_record> active_transaction_record::get_atr(std::shared_ptr<collection> collection,
                                                                                  const std::string& atr_id)
    {
        result res =
          collection->lookup_in(atr_id, { lookup_in_spec::get(ATR_FIELD_ATTEMPTS).xattr(), lookup_in_spec::get("$vbucket").xattr() });
        if (res.rc == LCB_ERR_DOCUMENT_NOT_FOUND) {
            return {};
        } else if (res.rc == LCB_SUCCESS) {
            nlohmann::json attempts = res.values[0].content_as<nlohmann::json>();
            return map_to_atr(collection, atr_id, res, attempts);
        } else {
            throw client_error(res);
        }
    }

} // namespace transactions
} // namespace couchbase
