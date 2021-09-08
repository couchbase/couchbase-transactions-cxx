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

#include "active_transaction_record.hxx"
#include "exceptions_internal.hxx"
#include "utils.hxx"
#include <couchbase/cluster.hxx>
#include <couchbase/errors.hxx>
#include <couchbase/operations.hxx>
#include <optional>

#include <future>
#include <memory>
#include <vector>

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

    inline std::optional<std::vector<doc_record>> active_transaction_record::process_document_ids(nlohmann::json& entry, std::string key)
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

    inline active_transaction_record active_transaction_record::map_to_atr(const couchbase::document_id& atr_id,
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
              atr_id.bucket(),
              atr_id.key(),
              element.key(),
              attempt_state_value(val[ATR_FIELD_STATUS].get<std::string>()),
              parse_mutation_cas(val.value(ATR_FIELD_START_TIMESTAMP, "")),
              parse_mutation_cas(val.value(ATR_FIELD_START_COMMIT, "")),
              parse_mutation_cas(val.value(ATR_FIELD_TIMESTAMP_COMPLETE, "")),
              parse_mutation_cas(val.value(ATR_FIELD_TIMESTAMP_ROLLBACK_START, "")),
              parse_mutation_cas(val.value(ATR_FIELD_TIMESTAMP_ROLLBACK_COMPLETE, "")),
              val.count(ATR_FIELD_EXPIRES_AFTER_MSECS) ? std::make_optional(val[ATR_FIELD_EXPIRES_AFTER_MSECS].get<std::uint32_t>())
                                                       : std::optional<std::uint32_t>(),
              process_document_ids(val, ATR_FIELD_DOCS_INSERTED),
              process_document_ids(val, ATR_FIELD_DOCS_REPLACED),
              process_document_ids(val, ATR_FIELD_DOCS_REMOVED),
              val.contains(ATR_FIELD_FORWARD_COMPAT) ? std::make_optional(val[ATR_FIELD_FORWARD_COMPAT].get<nlohmann::json>())
                                                     : std::nullopt,
              now_ns);
        }
        return active_transaction_record(atr_id, res.cas, std::move(entries));
    }

    // TODO: we should get the kv_timeout and put it in the request (pass in the transaction_config)
    std::optional<active_transaction_record> active_transaction_record::get_atr(cluster& cluster, const couchbase::document_id& atr_id)
    {
        couchbase::operations::lookup_in_request req{ atr_id };
        req.specs.add_spec(protocol::subdoc_opcode::get, true, ATR_FIELD_ATTEMPTS);
        req.specs.add_spec(protocol::subdoc_opcode::get, true, "$vbucket");
        // let's do a blocking lookup_in...
        auto barrier = std::make_shared<std::promise<result>>();
        auto f = barrier->get_future();
        cluster.execute(req, [barrier](couchbase::operations::lookup_in_response resp) mutable {
            barrier->set_value(result::create_from_subdoc_response<>(resp));
        });
        auto res = f.get();
        if (res.ec == couchbase::error::key_value_errc::document_not_found) {
            // that's ok, just return an empty one.
            return {};
        }
        if (!res.ec) {
            // success
            auto attempts = res.values[0].content_as<nlohmann::json>();
            return map_to_atr(atr_id, res, attempts);
        }
        // otherwise, raise an error.
        throw client_error(res);
    }

} // namespace transactions
} // namespace couchbase
