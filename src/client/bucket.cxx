#include <memory>

#include <boost/algorithm/string/split.hpp>
#include <spdlog/spdlog.h>

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
cb::bucket::find_or_create_collection(const std::string& collection)
{
    // TODO maybe more validation?
    std::vector<std::string> splits;
    boost::split(splits, collection, [](char c) { return c == '.'; });
    std::string scope_name("_default");
    std::string collection_name("_default");
    if (splits.size() == 2) {
        scope_name = splits[0];
        collection_name = splits[1];
    }
    auto it = std::find_if(collections_.begin(), collections_.end(), [&](const std::shared_ptr<cb::collection>& c) {
        return c->scope() == scope_name && c->name() == collection_name;
    });
    if (it == collections_.end()) {

        collections_.push_back(std::shared_ptr<cb::collection>(new cb::collection(shared_from_this(), scope_name, collection_name)));
        return collections_.back();
    }
    return *it;

}

std::shared_ptr<cb::collection>
cb::bucket::default_collection()
{
    return find_or_create_collection("_default._default");
}

std::shared_ptr<cb::collection>
cb::bucket::collection(const std::string& collection)
{
    return find_or_create_collection(collection);
}

cb::bucket::bucket(lcb_st* instance, const std::string& name)
  : lcb_(instance)
  , name_(name)
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

cb::bucket::~bucket() {
    close();
}

void
cb::bucket::close() {
    spdlog::trace("bucket shutting down - lcb_ = {}", (void*)lcb_);
    if (lcb_ != nullptr) {
        lcb_destroy(lcb_);
        lcb_ = nullptr;
    }
}
