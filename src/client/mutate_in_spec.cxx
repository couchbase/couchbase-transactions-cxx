#include <libcouchbase/couchbase.h>

#include <couchbase/client/mutate_in_spec.hxx>

couchbase::mutate_in_spec&
couchbase::mutate_in_spec::xattr()
{
    flags_ |= LCB_SUBDOCSPECS_F_XATTRPATH;
    return *this;
}

couchbase::mutate_in_spec&
couchbase::mutate_in_spec::create_path()
{
    flags_ |= LCB_SUBDOCSPECS_F_MKINTERMEDIATES;
    return *this;
}

couchbase::mutate_in_spec&
couchbase::mutate_in_spec::expand_macro()
{
    flags_ |= LCB_SUBDOCSPECS_F_XATTR_MACROVALUES;
    return *this;
}
