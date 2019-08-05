#include <iostream>
#include <stdexcept>

#include <libcouchbase/couchbase.h>
#include <libcouchbase/cluster.hxx>
#include <libcouchbase/bucket.hxx>
#include <utility>

couchbase::cluster::cluster(std::string cluster_address, std::string user_name, std::string password)
    : lcb_(nullptr), cluster_address_(std::move(cluster_address)), user_name_(std::move(user_name)), password_(std::move(password))
{
    connect();
}

couchbase::cluster::~cluster()
{
    shutdown();
}

couchbase::bucket *couchbase::cluster::open_bucket(const std::string &name)
{
    bucket *bkt = new bucket(lcb_, name);
    // TODO: cache buckets
    lcb_ = nullptr;
    return bkt;
}

void couchbase::cluster::connect()
{
    lcb_STATUS rc;

    struct lcb_create_st cfg {
    };
    cfg.version = 4;
    cfg.v.v4.connstr = cluster_address_.c_str();
    cfg.v.v4.io = nullptr;
    cfg.v.v4.logger = nullptr;
    cfg.v.v4.type = LCB_TYPE_CLUSTER;
    cfg.v.v4.username = nullptr;
    cfg.v.v4.passwd = nullptr;
    rc = lcb_create(&lcb_, &cfg);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to create libcouchbase instance: ") + lcb_strerror_short(rc));
    }

    lcb_AUTHENTICATOR *auth = lcbauth_new();
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
    rc = lcb_wait(lcb_);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to connect (wait) libcouchbase instance: ") + lcb_strerror_short(rc));
    }
}

void couchbase::cluster::shutdown()
{
    if (lcb_ != nullptr) {
        lcb_destroy(lcb_);
        lcb_ = nullptr;
    }
}
