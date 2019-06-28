#include <libcouchbase/lookup_in_spec.hxx>
#include <libcouchbase/couchbase.h>

couchbase::LookupInSpec::LookupInSpec(couchbase::LookupInSpecType type, const std::string &path = "") : type_(type), path_(path)
{
}

couchbase::LookupInSpec &couchbase::LookupInSpec::xattr()
{
    flags_ |= LCB_SUBDOCOPS_F_XATTRPATH;
    return *this;
}

couchbase::LookupInSpec couchbase::LookupInSpec::get(const std::string &path)
{
    return couchbase::LookupInSpec(LOOKUP_IN_GET, path);
}

couchbase::LookupInSpec couchbase::LookupInSpec::fulldoc_get()
{
    return couchbase::LookupInSpec(LOOKUP_IN_FULLDOC_GET);
}
