#include <couchbase/client/result.hxx>
#include <libcouchbase/couchbase.h>
#include <sstream>

std::string
couchbase::result::strerror() const
{
    return lcb_strerror_short(static_cast<lcb_STATUS>(rc));
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
