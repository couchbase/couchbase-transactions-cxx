/*
 *     Copyright 2021 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#include <memory>

#include "logging.hxx"
#include "pool.hxx"
#include <boost/algorithm/string/split.hpp>
#include <spdlog/fmt/ostr.h>

#include <couchbase/client/bucket.hxx>
#include <couchbase/client/collection.hxx>
#include <couchbase/support.hxx>
#include <libcouchbase/couchbase.h>

namespace cb = couchbase;

const std::string cb::bucket::default_name = "_default";

extern "C" {
static void
open_callback(lcb_INSTANCE* instance, lcb_STATUS status)
{
    auto* rc = static_cast<lcb_STATUS*>(const_cast<void*>(lcb_get_cookie(instance)));
    *rc = status;
}
}

// we expect this to be of the form "<scope>.<collection>", or "<collection>".
std::shared_ptr<cb::collection>
cb::bucket::find_or_create_collection(const std::string& collection)
{
    if (collection.empty()) {
        throw std::runtime_error("collection name is empty");
    }
    std::vector<std::string> splits;
    std::string scope_name;
    std::string collection_name;
    boost::split(splits, collection, [](char c) { return c == '.'; });
    switch (splits.size()) {
        case 2:
            scope_name = splits[0];
            collection_name = splits[1];
            break;
        case 1:
            collection_name = splits[0];
            break;
        default:
            throw std::runtime_error("malformed collection name");
    }
    if (scope_name.empty()) {
        scope_name = default_name;
    }
    if (collection_name.empty()) {
        collection_name = default_name;
    }
    std::unique_lock<std::mutex> lock(mutex_);
    auto it = std::find_if(collections_.begin(), collections_.end(), [&](const std::shared_ptr<cb::collection>& c) {
        return c->scope() == scope_name && c->name() == collection_name;
    });
    if (it == collections_.end()) {

        collections_.push_back(
          std::shared_ptr<cb::collection>(new cb::collection(shared_from_this(), scope_name, collection_name, default_kv_timeout())));
        return collections_.back();
    }
    return *it;
}

std::shared_ptr<cb::collection>
cb::bucket::default_collection()
{
    return find_or_create_collection(default_name + std::string(".") + default_name);
}

std::shared_ptr<cb::collection>
cb::bucket::collection(const std::string& collection)
{
    return find_or_create_collection(collection);
}

cb::bucket::bucket(std::unique_ptr<pool<lcb_st*>>& instance_pool, const std::string& name, std::chrono::microseconds kv_timeout)
  : name_(name)
  , kv_timeout_(kv_timeout)
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
        cb::client_log->trace("bucket {} opened successfully", name_);
        return lcb;
    });
    if (instance_pool_->size() == 0) {
        instance_pool_->release(instance_pool_->get());
    }
    client_log->info("opened bucket {}, max_instances={}", name, instance_pool_->max_size());
}

std::chrono::microseconds
cb::bucket::default_kv_timeout() const
{
    return kv_timeout_;
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
