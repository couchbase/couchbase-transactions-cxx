#include <couchbase/client/lookup_in_spec.hxx>
#include <utility>
#include <libcouchbase/couchbase.h>

namespace cb = couchbase;

cb::lookup_in_spec::lookup_in_spec(cb::lookup_in_spec_type type, std::string path = "") : type_(type), path_(std::move(path)), flags_(0)
{
}

cb::lookup_in_spec &cb::lookup_in_spec::xattr()
{
    flags_ |= LCB_SUBDOCOPS_F_XATTRPATH;
    return *this;
}

cb::lookup_in_spec cb::lookup_in_spec::get(const std::string &path)
{
    return cb::lookup_in_spec(LOOKUP_IN_GET, path);
}

cb::lookup_in_spec cb::lookup_in_spec::fulldoc_get()
{
    return cb::lookup_in_spec(LOOKUP_IN_FULLDOC_GET);
}
