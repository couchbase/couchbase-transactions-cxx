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
#include <optional>
#include <string>
#include <utility>

#include "result.hxx"
#include <couchbase/cluster.hxx>
#include <couchbase/transactions/exceptions.hxx>
#include <couchbase/transactions/transaction_config.hxx>

#include "couchbase/transactions/internal/atr_entry.hxx"
#include "couchbase/transactions/internal/utils.hxx"

namespace couchbase
{
namespace transactions
{
    class active_transaction_record
    {
      public:
        // TODO: we should get the kv_timeout and put it in the request (pass in the transaction_config)
        template<typename Callback>
        static void get_atr(cluster& cluster, const couchbase::document_id& atr_id, Callback&& cb)
        {
            couchbase::operations::lookup_in_request req{ atr_id };
            req.specs.add_spec(protocol::subdoc_opcode::get, true, ATR_FIELD_ATTEMPTS);
            req.specs.add_spec(protocol::subdoc_opcode::get, true, "$vbucket");
            cluster.execute(req, [atr_id, cb = std::move(cb)](couchbase::operations::lookup_in_response resp) {
                try {
                    if (resp.ctx.ec == couchbase::error::key_value_errc::document_not_found) {
                        // that's ok, just return an empty one.
                        return cb({}, {});
                    }
                    if (!resp.ctx.ec) {
                        // success
                        return cb(resp.ctx.ec, map_to_atr(resp));
                    }
                    // otherwise, raise an error.
                    cb(resp.ctx.ec, {});
                } catch (const std::exception& e) {
                    // ok - we have a corrupt ATR.  The question is:  what should we return for an error?
                    // Turns out, we don't much care in the code what this error is.  Since we cannot parse
                    // the atr, but there wasn't an error, lets select this one for now.
                    // TODO: consider a different mechanism - not an error_code.  Or, perhaps we need txn-specific
                    // error codes?
                    cb(couchbase::error::key_value_errc::path_invalid, std::nullopt);
                }
            });
        }

        static std::optional<active_transaction_record> get_atr(cluster& cluster, const couchbase::document_id& atr_id)
        {
            auto barrier = std::promise<std::optional<active_transaction_record>>();
            auto f = barrier.get_future();
            get_atr(cluster, atr_id, [&](std::error_code ec, std::optional<active_transaction_record> atr) {
                if (!ec) {
                    return barrier.set_value(atr);
                }
                return barrier.set_exception(std::make_exception_ptr(std::runtime_error(ec.message())));
            });
            return f.get();
        }
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
        couchbase::document_id id_;
        std::vector<atr_entry> entries_;

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
        static inline uint64_t parse_mutation_cas(const std::string& cas)
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

        static inline std::optional<std::vector<doc_record>> process_document_ids(nlohmann::json& entry, std::string key)
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
        static inline active_transaction_record map_to_atr(const couchbase::operations::lookup_in_response& resp)
        {
            std::vector<atr_entry> entries;
            if (resp.fields[0].status == protocol::status::success) {
                auto attempts = nlohmann::json::parse(resp.fields[0].value);
                auto vbucket = default_json_serializer::deserialize<nlohmann::json>(resp.fields[1].value);
                auto now_ns = now_ns_from_vbucket(vbucket);
                entries.reserve(attempts.size());
                for (auto& element : attempts.items()) {
                    auto& val = element.value();
                    entries.emplace_back(
                      resp.ctx.id.bucket(),
                      resp.ctx.id.key(),
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
                      now_ns,
                      val.contains(ATR_FIELD_DURABILITY_LEVEL) ? std::make_optional(val[ATR_FIELD_DURABILITY_LEVEL].get<nlohmann::json>())
                                                               : std::nullopt);
                }
            }
            return active_transaction_record(resp.ctx.id, resp.cas.value, std::move(entries));
        }
    };

} // namespace transactions
} // namespace couchbase
