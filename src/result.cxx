#include <libcouchbase/result.hxx>

couchbase::Result::Result(): rc(LCB_SUCCESS), cas(0), datatype(0), flags(0)
{
}

