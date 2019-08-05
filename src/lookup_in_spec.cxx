#include <libcouchbase/lookup_in_spec.hxx>
#include <utility>
#include <libcouchbase/couchbase.h>

couchbase::lookup_in_spec::lookup_in_spec(couchbase::lookup_in_spec_type type, std::string path = "") : type_(type), path_(std::move(path))
{
}

couchbase::lookup_in_spec &couchbase::lookup_in_spec::xattr()
{
    flags_ |= LCB_SUBDOCOPS_F_XATTRPATH;
    return *this;
}

couchbase::lookup_in_spec couchbase::lookup_in_spec::get(const std::string &path)
{
    return couchbase::lookup_in_spec(LOOKUP_IN_GET, path);
}

couchbase::lookup_in_spec couchbase::lookup_in_spec::fulldoc_get()
{
    return couchbase::lookup_in_spec(LOOKUP_IN_FULLDOC_GET);
}
