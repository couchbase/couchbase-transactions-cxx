#include <iostream>

#include <libcouchbase/couchbase.h>

#include <libcouchbase/bucket.hxx>

couchbase::collection *couchbase::bucket::default_collection()
{
    collection *col = new collection(this, "", "");
    // cache collection
    return col;
}

couchbase::bucket::bucket(lcb_st *instance, const std::string &name) : lcb_(instance)
{
    lcb_STATUS rc;

    rc = lcb_open(lcb_, name.c_str(), name.size());
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to open bucket (sched): ") + lcb_strerror_short(rc));
    }
    rc = lcb_wait(lcb_);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to open bucket (wait): ") + lcb_strerror_short(rc));
    }
}
