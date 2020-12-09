#include <memory>

#include <boost/algorithm/string/split.hpp>
#include <spdlog/fmt/ostr.h>
#include <spdlog/spdlog.h>

#include "pool.hxx"
#include <couchbase/client/bucket.hxx>
#include <couchbase/client/collection.hxx>
#include <couchbase/support.hxx>
#include <libcouchbase/couchbase.h>

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

cb::bucket::bucket(std::unique_ptr<Pool<lcb_st*>>& instance_pool, const std::string& name)
  : name_(name)
{
    instance_pool_ = std::move(instance_pool);
    instance_pool_->post_create_fn([this](lcb_st* lcb) -> lcb_st* {
        lcb_STATUS rc;
        lcb_set_open_callback(lcb, open_callback);
        lcb_set_cookie(lcb, &rc);
        rc = lcb_open(lcb, name_.c_str(), name_.size());
        if (rc != LCB_SUCCESS) {
            throw std::runtime_error(std::string("failed to open bucket (sched): ") + lcb_strerror_short(rc));
        }
        lcb_wait(lcb, LCB_WAIT_DEFAULT);
        if (rc != LCB_SUCCESS) {
            throw std::runtime_error(std::string("failed to open bucket (wait): ") + lcb_strerror_short(rc));
        }
        collection::install_callbacks(lcb);
        spdlog::trace("bucket {} opened successfully", name_);
        return lcb;
    });
    if (instance_pool_->size() == 0) {
        instance_pool_->release(instance_pool_->get());
    }
}

cb::bucket::~bucket()
{
    close();
}

void
cb::bucket::close()
{
}

size_t
cb::bucket::max_instances() const
{
    return instance_pool_->max_size();
}

size_t
cb::bucket::instances() const
{
    return instance_pool_->size();
}

size_t
cb::bucket::available_instances() const
{
    return instance_pool_->available();
}
