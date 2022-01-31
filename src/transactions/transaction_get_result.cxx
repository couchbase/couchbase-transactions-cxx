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
#include "result.hxx"
#include <couchbase/transactions/transaction_get_result.hxx>

namespace couchbase::transactions
{
transaction_get_result
transaction_get_result::create_from(const couchbase::operations::lookup_in_response& resp)
{
    std::optional<std::string> atr_id;
    std::optional<std::string> transaction_id;
    std::optional<std::string> attempt_id;
    std::optional<std::string> staged_content;
    std::optional<std::string> atr_bucket_name;
    std::optional<std::string> atr_scope_name;
    std::optional<std::string> atr_collection_name;
    std::optional<nlohmann::json> forward_compat;

    // read from xattrs.txn.restore
    std::optional<std::string> cas_pre_txn;
    std::optional<std::string> revid_pre_txn;
    std::optional<uint32_t> exptime_pre_txn;
    std::optional<std::string> crc32_of_staging;

    // read from $document
    std::optional<std::string> cas_from_doc;
    std::optional<std::string> revid_from_doc;
    std::optional<uint32_t> exptime_from_doc;
    std::optional<std::string> crc32_from_doc;

    std::optional<std::string> op;
    std::string content;

    if (resp.fields[0].status == protocol::status::success) {
        atr_id = default_json_serializer::deserialize_from_json_string<std::string>(resp.fields[0].value);
    }
    if (resp.fields[1].status == protocol::status::success) {
        transaction_id = default_json_serializer::deserialize_from_json_string<std::string>(resp.fields[1].value);
    }
    if (resp.fields[2].status == protocol::status::success) {
        attempt_id = default_json_serializer::deserialize_from_json_string<std::string>(resp.fields[2].value);
    }
    if (resp.fields[3].status == protocol::status::success) {
        staged_content = resp.fields[3].value;
    }
    if (resp.fields[4].status == protocol::status::success) {
        atr_bucket_name = default_json_serializer::deserialize_from_json_string<std::string>(resp.fields[4].value);
    }
    if (resp.fields[5].status == protocol::status::success) {
        atr_scope_name = default_json_serializer::deserialize_from_json_string<std::string>(resp.fields[5].value);
    }
    if (resp.fields[6].status == protocol::status::success) {
        atr_collection_name = default_json_serializer::deserialize_from_json_string<std::string>(resp.fields[6].value);
    }

    if (resp.fields[7].status == protocol::status::success) {
        auto restore = nlohmann::json::parse(resp.fields[7].value);
        cas_pre_txn = restore["CAS"].get<std::string>();
        // only present in 6.5+
        revid_pre_txn = restore["revid"].get<std::string>();
        exptime_pre_txn = restore["exptime"].get<uint32_t>();
    }
    if (resp.fields[8].status == protocol::status::success) {
        op = default_json_serializer::deserialize_from_json_string<std::string>(resp.fields[8].value);
    }
    if (resp.fields[9].status == protocol::status::success) {
        auto doc = nlohmann::json::parse(resp.fields[9].value);
        cas_from_doc = doc["CAS"].get<std::string>();
        // only present in 6.5+
        revid_from_doc = doc["revid"].get<std::string>();
        exptime_from_doc = doc["exptime"].get<uint32_t>();
        crc32_from_doc = doc["value_crc32c"].get<std::string>();
    }
    if (resp.fields[10].status == protocol::status::success) {
        crc32_of_staging = default_json_serializer::deserialize_from_json_string<std::string>(resp.fields[10].value);
    }
    if (resp.fields[11].status == protocol::status::success) {
        forward_compat = nlohmann::json::parse(resp.fields[11].value);
    } else {
        forward_compat = nlohmann::json::object();
    }
    if (resp.fields[12].status == protocol::status::success) {
        content = resp.fields[12].value;
    }

    transaction_links links(atr_id,
                            atr_bucket_name,
                            atr_scope_name,
                            atr_collection_name,
                            transaction_id,
                            attempt_id,
                            staged_content,
                            cas_pre_txn,
                            revid_pre_txn,
                            exptime_pre_txn,
                            crc32_of_staging,
                            op,
                            forward_compat,
                            resp.deleted);
    document_metadata md(cas_from_doc, revid_from_doc, exptime_from_doc, crc32_from_doc);
    return { resp.ctx.id, content, resp.cas.value, links, std::make_optional(md) };
}

transaction_get_result
transaction_get_result::create_from(const couchbase::document_id& id, const result& res)
{
    std::optional<std::string> atr_id;
    std::optional<std::string> transaction_id;
    std::optional<std::string> attempt_id;
    std::optional<std::string> staged_content;
    std::optional<std::string> atr_bucket_name;
    std::optional<std::string> atr_scope_name;
    std::optional<std::string> atr_collection_name;
    std::optional<nlohmann::json> forward_compat;

    // read from xattrs.txn.restore
    std::optional<std::string> cas_pre_txn;
    std::optional<std::string> revid_pre_txn;
    std::optional<uint32_t> exptime_pre_txn;
    std::optional<std::string> crc32_of_staging;

    // read from $document
    std::optional<std::string> cas_from_doc;
    std::optional<std::string> revid_from_doc;
    std::optional<uint32_t> exptime_from_doc;
    std::optional<std::string> crc32_from_doc;

    std::optional<std::string> op;
    std::string content;

    if (res.values[0].has_value()) {
        atr_id = res.values[0].content_as<std::string>();
    }
    if (res.values[1].has_value()) {
        transaction_id = res.values[1].content_as<std::string>();
    }
    if (res.values[2].has_value()) {
        attempt_id = res.values[2].content_as<std::string>();
    }
    if (res.values[3].has_value()) {
        staged_content = res.values[3].content_as<nlohmann::json>().dump();
    }
    if (res.values[4].has_value()) {
        atr_bucket_name = res.values[4].content_as<std::string>();
    }
    if (res.values[5].has_value()) {
        atr_scope_name = res.values[5].content_as<std::string>();
    }
    if (res.values[6].has_value()) {
        atr_collection_name = res.values[6].content_as<std::string>();
    }
    if (res.values[7].has_value()) {
        auto restore = res.values[7].content_as<nlohmann::json>();
        cas_pre_txn = restore["CAS"].get<std::string>();
        // only present in 6.5+
        revid_pre_txn = restore["revid"].get<std::string>();
        exptime_pre_txn = restore["exptime"].get<uint32_t>();
    }
    if (res.values[8].has_value()) {
        op = res.values[8].content_as<std::string>();
    }
    if (res.values[9].has_value()) {
        auto doc = res.values[9].content_as<nlohmann::json>();
        cas_from_doc = doc["CAS"].get<std::string>();
        // only present in 6.5+
        revid_from_doc = doc["revid"].get<std::string>();
        exptime_from_doc = doc["exptime"].get<uint32_t>();
        crc32_from_doc = doc["value_crc32c"].get<std::string>();
    }
    if (res.values[10].has_value()) {
        crc32_of_staging = res.values[10].content_as<std::string>();
    }
    if (res.values[11].has_value()) {
        forward_compat = res.values[11].content_as<nlohmann::json>();
    } else {
        forward_compat = nlohmann::json::object();
    }
    if (res.values[12].has_value()) {
        content = res.values[12].content_as<nlohmann::json>().dump();
    }

    transaction_links links(atr_id,
                            atr_bucket_name,
                            atr_scope_name,
                            atr_collection_name,
                            transaction_id,
                            attempt_id,
                            staged_content,
                            cas_pre_txn,
                            revid_pre_txn,
                            exptime_pre_txn,
                            crc32_of_staging,
                            op,
                            forward_compat,
                            res.is_deleted);
    document_metadata md(cas_from_doc, revid_from_doc, exptime_from_doc, crc32_from_doc);
    return { id, content, res.cas, links, std::make_optional(md) };
}
}; // namespace couchbase::transactions