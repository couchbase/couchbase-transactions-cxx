#include <couchbase/client/bucket.hxx>
#include <couchbase/client/cluster.hxx>
#include <couchbase/client/result.hxx>
#include <couchbase/support.hxx>
#include <libcouchbase/couchbase.h>
#include <memory>
#include <spdlog/fmt/ostr.h>
#include <spdlog/spdlog.h>
#include <stdexcept>
#include <utility>

#include "pool.hxx"

namespace cb = couchbase;

void
shutdown(lcb_st* lcb)
{
    if (lcb == nullptr) {
        return;
    }
    lcb_destroy(lcb);
}

lcb_st*
connect(const std::string& cluster_address, const std::string& user_name, const std::string& password)
{
    lcb_st* lcb = nullptr;
    lcb_STATUS rc;
    lcb_CREATEOPTS* opts;
    lcb_createopts_create(&opts, LCB_TYPE_CLUSTER);
    lcb_createopts_connstr(opts, cluster_address.c_str(), cluster_address.size());
    rc = lcb_create(&lcb, opts);
    lcb_createopts_destroy(opts);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to create libcouchbase instance: ") + lcb_strerror_short(rc));
    }

    lcb_AUTHENTICATOR* auth = lcbauth_new();
    lcbauth_set_mode(auth, LCBAUTH_MODE_RBAC);
    rc = lcbauth_add_pass(auth, user_name.c_str(), password.c_str(), LCBAUTH_F_CLUSTER);
    if (rc != LCB_SUCCESS) {
        lcbauth_unref(auth);
        throw std::runtime_error(std::string("failed to build credentials for authenticator: ") + lcb_strerror_short(rc));
    }
    lcb_set_auth(lcb, auth);
    lcbauth_unref(auth);

    rc = lcb_connect(lcb);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to connect (sched) libcouchbase instance: ") + lcb_strerror_short(rc));
    }
    rc = lcb_wait(lcb, LCB_WAIT_DEFAULT);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to connect (wait) libcouchbase instance: ") + lcb_strerror_short(rc));
    }
    spdlog::trace("cluster connection successful, returning {}", (void*)lcb);
    return lcb;
}

cb::cluster::cluster(std::string cluster_address, std::string user_name, std::string password, const cluster_options& opts)
  : cluster_address_(std::move(cluster_address))
  , user_name_(std::move(user_name))
  , password_(std::move(password))
  , max_bucket_instances_(opts.max_bucket_instances())
{
    instance_pool_ = std::unique_ptr<Pool<lcb_st*>>(
      new Pool<lcb_st*>(opts.max_instances(), [&] { return connect(cluster_address_, user_name_, password_); }, shutdown));
    spdlog::info("couchbase client library {} attempting to connect to {}", VERSION_STR, cluster_address_);

    // TODO: ponder this - should we connect _now_, or wait until first use?
    // for now, lets get it and release back to pool
    instance_pool_->release(instance_pool_->get());
}

cb::cluster::cluster(const cluster& cluster)
  : cluster_address_(cluster.cluster_address_)
  , user_name_(cluster.user_name_)
  , password_(cluster.password_)
  , max_bucket_instances_(cluster.max_bucket_instances_)
{
    instance_pool_ = cluster.instance_pool_->clone(max_bucket_instances_);
    spdlog::info("couchbase client library {} attempting to connect to {}", VERSION_STR, cluster_address_);
    instance_pool_->release(instance_pool_->get());
}

bool
cb::cluster::operator==(const cluster& other) const
{
    return this == &other;
}

cb::cluster::~cluster()
{
}

std::shared_ptr<cb::bucket>
cb::cluster::bucket(const std::string& name)
{
    std::unique_lock<std::mutex> lock(mutex_);
    spdlog::trace("open buckets before:");
    for (auto& b : open_buckets_) {
        spdlog::trace("{}", *b);
    }
    auto it =
      std::find_if(open_buckets_.begin(), open_buckets_.end(), [&](const std::shared_ptr<cb::bucket>& b) { return b->name() == name; });
    if (it != open_buckets_.end()) {
        spdlog::trace("second look found {} already opened, returning", name);
        return *it;
    } else {
        // clone the pool, add lcb to it
        spdlog::trace("will create bucket {} now...", name);
        auto bucket_pool = instance_pool_->clone(max_bucket_instances_);
        instance_pool_->swap_available(*bucket_pool, true);
        // create the bucket, push into the bucket list...
        auto b = std::shared_ptr<cb::bucket>(new cb::bucket(bucket_pool, name));
        open_buckets_.push_back(b);
        spdlog::trace("open buckets after :");
        for (auto& b : open_buckets_) {
            spdlog::trace("{}", *b);
        }
        return b;
    }
}

extern "C" {
static void
http_callback(lcb_INSTANCE*, int, const lcb_RESPHTTP* resp)
{
    cb::result* res = nullptr;
    lcb_resphttp_cookie(resp, reinterpret_cast<void**>(&res));
    res->rc = lcb_resphttp_status(resp);
    if (res->rc == LCB_SUCCESS) {
        const char* data = nullptr;
        size_t ndata = 0;
        lcb_resphttp_body(resp, &data, &ndata);
        res->value = nlohmann::json::parse(data, data + ndata);
    }
}
}

std::list<std::string>
cb::cluster::buckets()
{
    return instance_pool_->wrap_access<std::list<std::string>>([&](lcb_st* lcb) -> std::list<std::string> {
        std::unique_lock<std::mutex> lock(mutex_);
        std::string path("/pools/default/buckets");
        lcb_CMDHTTP* cmd;
        lcb_cmdhttp_create(&cmd, lcb_HTTP_TYPE::LCB_HTTP_TYPE_MANAGEMENT);
        lcb_cmdhttp_method(cmd, lcb_HTTP_METHOD::LCB_HTTP_METHOD_GET);
        lcb_cmdhttp_path(cmd, path.data(), path.size());
        lcb_install_callback(lcb, LCB_CALLBACK_HTTP, (lcb_RESPCALLBACK)http_callback);
        cb::result res;
        lcb_http(lcb, &res, cmd);
        lcb_cmdhttp_destroy(cmd);
        lcb_wait(lcb, LCB_WAIT_DEFAULT);
        if (res.rc != LCB_SUCCESS) {
            throw std::runtime_error(std::string("failed to retrieve list of buckets: ") + res.strerror());
        }
        std::list<std::string> names;
        if (res.value) {
            for (const auto& it : *res.value) {
                names.push_back(it["name"].get<std::string>());
            }
        }
        return names;
    });
}
size_t
cb::cluster::max_instances() const
{
    return instance_pool_->max_size();
}
size_t
cb::cluster::instances() const
{
    return instance_pool_->size();
}
size_t
cb::cluster::available_instances() const
{
    return instance_pool_->available();
}
