#include <sstream>
#include <libcouchbase/couchbase.h>
#include <couchbase/client/result.hxx>

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

std::string
couchbase::result::to_string() const
{
    std::ostringstream os;
    os << "result{";
    os << "rc:" << rc << ",";
    os << "strerror:" << strerror() << ",";
    os << "cas:" << cas << ",";
    os << "datatype:" << datatype << ",";
    os << "flags:" << flags << ",";
    os << "value:";
    if (value) {
        os << value->dump();
    }
    os << ",";
    os << "values:";
    if (!values.empty()) {
        os << "[";
        for(auto& v: values) {
            os << v->dump() << ",";
        }
        os << "[";
    }
    os << "}";
    return os.str();
}
