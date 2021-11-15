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

#include <couchbase/internal/nlohmann/json.hpp>
#include <couchbase/support.hxx>
#include <couchbase/transactions/transcoder.hxx>
#include <optional>
#include <string>
#include <vector>

/**
 * @file
 * Provides an object encapsulating the results of a kv operation in Couchbase Transactions Client.
 */
namespace couchbase
{
namespace transactions
{
    struct result_base {
        std::string raw_value;
        std::error_code ec;

        result_base() = default;

        result_base(const std::string& res)
          : raw_value(res)
        {
        }

        bool has_value() const
        {
            return !raw_value.empty();
        }
    };

    /**
     * @brief Result of a subdoc operation.
     *
     * See @ref collection.lookup_in and @ref collection.mutate_in
     */
    struct subdoc_result : result_base {
        enum class status_type : uint16_t {
            success = 0x00,
            subdoc_path_not_found = 0xc0,
            subdoc_path_mismatch = 0xc1,
            subdoc_path_invalid = 0xc2,
            subdoc_path_too_big = 0xc3,
            subdoc_doc_too_deep = 0xc4,
            subdoc_value_cannot_insert = 0xc5,
            subdoc_doc_not_json = 0xc6,
            subdoc_num_range_error = 0xc7,
            subdoc_delta_invalid = 0xc8,
            subdoc_path_exists = 0xc9,
            subdoc_value_too_deep = 0xca,
            subdoc_invalid_combo = 0xcb,
            subdoc_multi_path_failure = 0xcc,
            subdoc_success_deleted = 0xcd,
            subdoc_xattr_invalid_flag_combo = 0xce,
            subdoc_xattr_invalid_key_combo = 0xcf,
            subdoc_xattr_unknown_macro = 0xd0,
            subdoc_xattr_unknown_vattr = 0xd1,
            subdoc_xattr_cannot_modify_vattr = 0xd2,
            subdoc_multi_path_failure_deleted = 0xd3,
            subdoc_invalid_xattr_order = 0xd4,
        };
        status_type status;

        subdoc_result()
          : result_base()
          , status(status_type::success)
        {
        }
        subdoc_result(uint32_t s)
          : status(static_cast<status_type>(s))
        {
        }
        subdoc_result(const std::string& v, uint32_t s)
          : result_base(v)
          , status(static_cast<status_type>(s))
        {
        }

        template<typename T>
        T content_as() const
        {
            // this will always be a json string.  To not have extraneous
            // "" when asked to return as a string, we parse it first.
            // NOTE: lets do better.
            return nlohmann::json::parse(raw_value).get<T>();
        }
    };

    /**
     * @brief The result of an operation on a cluster.
     *
     * This encapsulates the server response to an operation.  For example:
     *
     * @code{.cpp}
     * std::string key = "somekey";
     * result res = collection->get(key);
     * if (res.is_success()) {
     *     auto doc = res.value.get<nlohmann::json>();
     * } else {
     *     cerr << "error getting " << key << ":" << res.strerror();
     * }
     * @endcode
     *
     * If the operation returns multiple results, like a lookup_in, then
     * @ref result::values is used instead:
     *
     * @code{.cpp}
     * std::string key = "somekey";
     * result res = collection->lookup_in(key, {lookup_in_spec::get("name"), lookup_in_spec::get("address")});
     * if (res.is_success()) {
     *     auto name = res.values[0].value->get<std::string>();
     *     auto address = res.values[1].value->get<std::string>();
     * } else {
     *     cerr << "error getting " << key << ":" << res.strerror();
     * }
     * @endcode
     *
     * If you define a to_json and from_json on an object, you can serialize/deserialize into it directly:
     *
     * @code{.cpp}
     * struct Person {
     *     std::string name;
     *     std::string address
     * };
     *
     * void
     * to_json(nlohmann::json& j, const Person& p) {
     *    j = nlohmann::json({ {"name", p.name} , {"address", p.address}};
     * }
     *
     * void
     * from_json(const nlohmann::json& j, Person& p) {
     *    j.at("name").get_to(p.name);
     *    j.at("address").get_to(p.address);
     * }
     *
     * auto res = collection.get(key);
     * if (res.is_success()) {
     *     Person p = res.value->get<Person>();
     *     cout << "name=" << p.name << ",address=" << p.address;
     * } else {
     *     cerr << "error getting " << key << ":" << res.strerror();
     * }
     * @endcode
     */

    struct result : result_base {
        /** @brief return code for operation */
        uint32_t rc;
        /** @brief CAS for document, if any */
        uint64_t cas;
        /** @brief datatype flag for content */
        uint8_t datatype;
        uint32_t flags;
        /** @brief document key */
        std::string key;
        /** @brief results of subdoc spec operations */
        std::vector<subdoc_result> values;
        bool is_deleted;
        bool ignore_subdoc_errors;

        template<typename Resp>
        static result create_from_mutation_response(const Resp& resp)
        {
            result res{};
            res.ec = resp.ctx.ec;
            res.cas = resp.cas.value;
            res.key = resp.ctx.id.key();
            return res;
        }

        template<typename Resp>
        static result create_from_response(const Resp& resp)
        {
            result res{};
            res.ec = resp.ctx.ec;
            res.cas = resp.cas.value;
            res.key = resp.ctx.id.key();
            res.flags = resp.flags;
            res.raw_value = resp.value;
            return res;
        }

        template<typename Resp>
        static result create_from_subdoc_response(const Resp& resp)
        {
            result res{};
            res.ec = resp.ctx.ec;
            res.cas = resp.cas.value;
            res.key = resp.ctx.id.key();
            res.is_deleted = resp.deleted;
            for (auto& field : resp.fields) {
                res.values.emplace_back(field.value, static_cast<uint32_t>(field.status));
            }
            return res;
        }

        result()
          : result_base()
          , rc(0)
          , cas(0)
          , datatype(0)
          , flags(0)
          , is_deleted(0)
          , ignore_subdoc_errors(0)
        {
        }
        /**
         * Get description of error
         *
         * @return String describing the error code.
         */
        CB_NODISCARD std::string strerror() const;
        CB_NODISCARD bool is_success() const;
        /**
         *  Get error code.  This is either the rc_, or if that is LCB_SUCCESS,
         *  then the first error in the values (if any) see @ref subdoc_results
         */
        // CB_NODISCARD uint32_t error() const;
        template<typename OStream>
        friend OStream& operator<<(OStream& os, const result& res)
        {
            os << "result{";
            os << "rc:" << res.rc << ",";
            os << "strerror:" << res.strerror() << ",";
            os << "cas:" << res.cas << ",";
            os << "is_deleted:" << res.is_deleted << ",";
            os << "datatype:" << res.datatype << ",";
            os << "flags:" << res.flags << ",";
            os << "raw_value" << res.raw_value;
            if (!res.values.empty()) {
                os << ",values:[";
                for (auto& v : res.values) {
                    os << "{" << v.raw_value << "," << static_cast<uint32_t>(v.status) << "},";
                }
                os << "]";
            }
            os << "}";
            return os;
        }

        template<typename T>
        T content_as() const
        {
            return default_json_serializer::deserialize<T>(raw_value);
        }
        CB_NODISCARD subdoc_result::status_type subdoc_status() const;
    };
} // namespace transactions
} // namespace couchbase
