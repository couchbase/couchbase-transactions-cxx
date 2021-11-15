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
#include <couchbase/errors.hxx>
#include <couchbase/transactions/transcoder.hxx>
#include <sstream>

namespace tx = couchbase::transactions;

std::string
tx::result::strerror() const
{
    static std::string success("success");
    if (ec) {
        return ec.message();
    }
    return success;
}

bool
tx::result::is_success() const
{
    return !ec;
}

tx::subdoc_result::status_type
tx::result::subdoc_status() const
{
    auto it = std::find_if(
      values.begin(), values.end(), [](const subdoc_result& res) { return res.status != subdoc_result::status_type::success; });
    if (it != values.end()) {
        return it->status;
    }
    return subdoc_result::status_type::success;
}
