#include <couchbase/client/result.hxx>
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
