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

#include <couchbase/client/result.hxx>
#include <couchbase/client/transcoder.hxx>
#include <libcouchbase/couchbase.h>
#include <sstream>

std::string
couchbase::result::strerror() const
{
    return lcb_strerror_short(static_cast<lcb_STATUS>(error()));
}

bool
couchbase::result::is_not_found() const
{
    return rc == LCB_ERR_DOCUMENT_NOT_FOUND;
}

bool
couchbase::result::is_success() const
{
    return rc == LCB_SUCCESS;
}

bool
couchbase::result::is_value_too_large() const
{
    return rc == LCB_ERR_VALUE_TOO_LARGE;
}

bool
couchbase::result::is_timeout() const
{
    return rc == LCB_ERR_TIMEOUT;
}

uint32_t
couchbase::result::error() const
{
    if (rc != LCB_SUCCESS || ignore_subdoc_errors) {
        return rc;
    }
    for (auto v : values) {
        if (v.status != LCB_SUCCESS) {
            return v.status;
        }
    }
    return rc;
}
