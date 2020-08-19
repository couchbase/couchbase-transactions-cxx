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
#include <couchbase/support.hxx>
#include <nlohmann/json.hpp>
#include <string>
#include <vector>

namespace couchbase
{
struct result {
    uint32_t rc;
    uint64_t cas;
    uint8_t datatype;
    uint32_t flags;
    std::string key;
    boost::optional<nlohmann::json> value;
    std::vector<boost::optional<nlohmann::json>> values;

    CB_NODISCARD std::string strerror() const;
    CB_NODISCARD bool is_not_found() const;
    CB_NODISCARD bool is_success() const;
    CB_NODISCARD bool is_value_too_large() const;
    CB_NODISCARD std::string to_string() const;
};
} // namespace couchbase
