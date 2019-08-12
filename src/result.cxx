#include <libcouchbase/result.hxx>

couchbase::result::result() : rc(LCB_SUCCESS), cas(0), datatype(0), flags(0), value(nullptr), values(0)
{
}
