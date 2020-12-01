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

#include <boost/optional.hpp>
#include <couchbase/internal/nlohmann/json.hpp>
#include <couchbase/support.hxx>
#include <string>
#include <vector>

namespace couchbase
{
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

struct subdoc_result {
    boost::optional<nlohmann::json> value;
    uint32_t status;

    subdoc_result()
      : status(0)
    {
    }
    subdoc_result(uint32_t s)
      : status(s)
    {
    }
    subdoc_result(nlohmann::json v, uint32_t s)
      : value(v)
      , status(s)
    {
    }
};

struct result {
    uint32_t rc;
    uint64_t cas;
    uint8_t datatype;
    uint32_t flags;
    std::string key;
    boost::optional<nlohmann::json> value;
    std::vector<subdoc_result> values;
    bool is_deleted;
    bool ignore_subdoc_errors;

    result()
      : rc(0)
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
    CB_NODISCARD bool is_not_found() const;
    CB_NODISCARD bool is_success() const;
    CB_NODISCARD bool is_value_too_large() const;
    /**
     *  Get error code.  This is either the rc_, or if that is LCB_SUCCESS,
     *  then the first error in the values (if any) see @ref subdoc_results
     */
    CB_NODISCARD uint32_t error() const;
    template<typename OStream>
    friend OStream& operator<<(OStream& os, const result& res)
    {
        os << "result{";
        os << "rc:" << res.rc << ",";
        os << "strerror:" << res.strerror() << ",";
        os << "cas:" << res.cas << ",";
        os << "is_deleted:" << res.is_deleted << ",";
        os << "datatype:" << res.datatype << ",";
        os << "flags:" << res.flags;
        if (res.value) {
            os << ",value:";
            os << res.value->dump();
        }
        if (!res.values.empty()) {
            os << ",values:[";
            for (auto& v : res.values) {
                os << "{" << (v.value ? v.value->dump() : "") << "," << v.status << "},";
            }
            os << "]";
        }
        os << "}";
        return os;
    }
};
} // namespace couchbase
