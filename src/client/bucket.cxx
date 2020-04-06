#include <iostream>
#include <memory>

#include <boost/algorithm/string/split.hpp>

#include <libcouchbase/couchbase.h>

#include <couchbase/client/bucket.hxx>
#include <couchbase/client/collection.hxx>

namespace cb = couchbase;

extern "C" {
static void
open_callback(lcb_INSTANCE* instance, lcb_STATUS status)
{
    auto* rc = static_cast<lcb_STATUS*>(const_cast<void*>(lcb_get_cookie(instance)));
    *rc = status;
}
}

std::shared_ptr<cb::collection>
cb::bucket::default_collection()
{
    return std::make_shared<cb::collection>(shared_from_this(), "", "");
}

std::shared_ptr<cb::collection>
cb::bucket::collection(const std::string& collection)
{
    std::vector<std::string> splits;
    boost::split(splits, collection, ".");
    std::string scope_name("_default");
    std::string collection_name("_default");
    if (splits.size() == 2) {
        scope_name = splits[0];
        collection_name = splits[1];
    }
    return std::make_shared<cb::collection>(shared_from_this(), scope_name, collection_name);
}

cb::bucket::bucket(lcb_st* instance, const std::string& name)
  : lcb_(instance)
{
    lcb_STATUS rc;

    lcb_set_open_callback(lcb_, open_callback);
    lcb_set_cookie(lcb_, &rc);
    rc = lcb_open(lcb_, name.c_str(), name.size());
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to open bucket (sched): ") + lcb_strerror_short(rc));
    }
    lcb_wait(lcb_, LCB_WAIT_DEFAULT);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to open bucket (wait): ") + lcb_strerror_short(rc));
    }
}
