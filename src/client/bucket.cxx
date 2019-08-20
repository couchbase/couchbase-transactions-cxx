#include <iostream>

#include <libcouchbase/couchbase.h>

#include <couchbase/client/bucket.hxx>

namespace cb = couchbase;

extern "C" {
static void open_callback(lcb_INSTANCE *instance, lcb_STATUS status)
{
    lcb_STATUS *rc = (lcb_STATUS *)lcb_get_cookie(instance);
    *rc = status;
}
}

cb::collection *cb::bucket::default_collection()
{
    collection *col = new collection(this, "", "");
    // cache collection
    return col;
}

cb::bucket::bucket(lcb_st *instance, const std::string &name) : lcb_(instance)
{
    lcb_STATUS rc;

    lcb_set_open_callback(lcb_, open_callback);
    lcb_set_cookie(lcb_, &rc);
    rc = lcb_open(lcb_, name.c_str(), name.size());
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to open bucket (sched): ") + lcb_strerror_short(rc));
    }
    lcb_wait(lcb_);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to open bucket (wait): ") + lcb_strerror_short(rc));
    }
}
