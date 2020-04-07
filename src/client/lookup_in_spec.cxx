#include <libcouchbase/couchbase.h>

#include <couchbase/client/lookup_in_spec.hxx>

couchbase::lookup_in_spec&
couchbase::lookup_in_spec::xattr()
{
    flags_ |= LCB_SUBDOCSPECS_F_XATTRPATH;
    return *this;
}
