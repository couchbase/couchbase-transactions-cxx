#include <memory>
#include <stdexcept>
#include <utility>

#include <couchbase/client/bucket.hxx>
#include <couchbase/client/cluster.hxx>
#include <couchbase/client/result.hxx>
#include <libcouchbase/couchbase.h>

namespace cb = couchbase;

cb::cluster::cluster(std::string cluster_address, std::string user_name, std::string password)
  : lcb_(nullptr)
  , cluster_address_(std::move(cluster_address))
  , user_name_(std::move(user_name))
  , password_(std::move(password))
{
    connect();
}

cb::cluster::~cluster()
{
    shutdown();
}

std::shared_ptr<cb::bucket>
cb::cluster::bucket(const std::string& name)
{
    connect();
    std::unique_lock<std::mutex> lock(mutex_);
    auto bkt = bucket::create(lcb_, name);
    // TODO: cache buckets
    lcb_ = nullptr;
    return bkt;
}

void
cb::cluster::connect()
{
    std::unique_lock<std::mutex> lock(mutex_);
    if (lcb_ != nullptr) {
        return;
    }
    lcb_STATUS rc;
    lcb_CREATEOPTS* opts;
    lcb_createopts_create(&opts, LCB_TYPE_CLUSTER);
    lcb_createopts_connstr(opts, cluster_address_.c_str(), cluster_address_.size());
    rc = lcb_create(&lcb_, opts);
    lcb_createopts_destroy(opts);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to create libcouchbase instance: ") + lcb_strerror_short(rc));
    }

    lcb_AUTHENTICATOR* auth = lcbauth_new();
    lcbauth_set_mode(auth, LCBAUTH_MODE_RBAC);
    rc = lcbauth_add_pass(auth, user_name_.c_str(), password_.c_str(), LCBAUTH_F_CLUSTER);
    if (rc != LCB_SUCCESS) {
        lcbauth_unref(auth);
        throw std::runtime_error(std::string("failed to build credentials for authenticator: ") + lcb_strerror_short(rc));
    }
    lcb_set_auth(lcb_, auth);
    lcbauth_unref(auth);

    rc = lcb_connect(lcb_);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to connect (sched) libcouchbase instance: ") + lcb_strerror_short(rc));
    }
    rc = lcb_wait(lcb_, LCB_WAIT_DEFAULT);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to connect (wait) libcouchbase instance: ") + lcb_strerror_short(rc));
    }
}

void
cb::cluster::shutdown()
{
    if (lcb_ != nullptr) {
        lcb_destroy(lcb_);
        lcb_ = nullptr;
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
    connect();
    std::unique_lock<std::mutex> lock(mutex_);
    std::string path("/pools/default/buckets");
    lcb_CMDHTTP* cmd;
    lcb_cmdhttp_create(&cmd, lcb_HTTP_TYPE::LCB_HTTP_TYPE_MANAGEMENT);
    lcb_cmdhttp_method(cmd, lcb_HTTP_METHOD::LCB_HTTP_METHOD_GET);
    lcb_cmdhttp_path(cmd, path.data(), path.size());
    lcb_install_callback(lcb_, LCB_CALLBACK_HTTP, (lcb_RESPCALLBACK)http_callback);
    cb::result res;
    lcb_http(lcb_, &res, cmd);
    lcb_cmdhttp_destroy(cmd);
    lcb_wait(lcb_, LCB_WAIT_DEFAULT);
    if (res.rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to retrieve list of buckets: ") + res.strerror());
    }
    std::list<std::string> names;
    for (const auto& it : *res.value) {
        names.push_back(it["name"].get<std::string>());
    }
    return names;
}
