#include <iostream>
#include <utility>

#include <couchbase/client/bucket.hxx>
#include <couchbase/client/collection.hxx>
#include <couchbase/client/lookup_in_spec.hxx>

#include <libcouchbase/couchbase.h>

namespace cb = couchbase;

extern "C" {
static void
store_callback(lcb_INSTANCE*, int, const lcb_RESPSTORE* resp)
{
    cb::result* res = nullptr;
    lcb_respstore_cookie(resp, reinterpret_cast<void**>(&res));
    res->rc = lcb_respstore_status(resp);
    lcb_respstore_cas(resp, &res->cas);
    const char* data = nullptr;
    size_t ndata = 0;
    lcb_respstore_key(resp, &data, &ndata);
    res->key = std::string(data, ndata);
}

static void
get_callback(lcb_INSTANCE*, int, const lcb_RESPGET* resp)
{
    cb::result* res = nullptr;
    lcb_respget_cookie(resp, reinterpret_cast<void**>(&res));
    res->rc = lcb_respget_status(resp);
    if (res->rc == LCB_SUCCESS) {
        lcb_respget_cas(resp, &res->cas);
        lcb_respget_datatype(resp, &res->datatype);
        lcb_respget_flags(resp, &res->flags);

        const char* data = nullptr;
        size_t ndata = 0;
        lcb_respget_key(resp, &data, &ndata);
        res->key = std::string(data, ndata);
        lcb_respget_value(resp, &data, &ndata);
        res->value.emplace(nlohmann::json::parse(data, data + ndata));
    }
}

static void
remove_callback(lcb_INSTANCE*, int, const lcb_RESPREMOVE* resp)
{
    cb::result* res = nullptr;
    lcb_respremove_cookie(resp, reinterpret_cast<void**>(&res));
    res->rc = lcb_respremove_status(resp);
    lcb_respremove_cas(resp, &res->cas);
    const char* data = nullptr;
    size_t ndata = 0;
    lcb_respremove_key(resp, &data, &ndata);
    res->key = std::string(data, ndata);
}

static void
subdoc_callback(lcb_INSTANCE*, int, const lcb_RESPSUBDOC* resp)
{
    cb::result* res = nullptr;
    lcb_respsubdoc_cookie(resp, reinterpret_cast<void**>(&res));
    res->rc = lcb_respsubdoc_status(resp);
    lcb_respsubdoc_cas(resp, &res->cas);
    const char* data = nullptr;
    size_t ndata = 0;
    lcb_respsubdoc_key(resp, &data, &ndata);
    res->key = std::string(data, ndata);

    size_t len = lcb_respsubdoc_result_size(resp);
    res->values.reserve(len);
    for (size_t idx = 0; idx < len; idx++) {
        data = nullptr;
        ndata = 0;
        lcb_respsubdoc_result_value(resp, idx, &data, &ndata);
        if (data) {
            res->values[idx].emplace(nlohmann::json::parse(data, data + ndata));
        }
    }
}
}

cb::collection::collection(std::shared_ptr<bucket> bucket, std::string scope, std::string name)
  : bucket_(std::move(bucket))
  , scope_(std::move(scope))
  , name_(std::move(name))
{
    lcb_install_callback(bucket_->lcb_, LCB_CALLBACK_STORE, reinterpret_cast<lcb_RESPCALLBACK>(store_callback));
    lcb_install_callback(bucket_->lcb_, LCB_CALLBACK_GET, reinterpret_cast<lcb_RESPCALLBACK>(get_callback));
    lcb_install_callback(bucket_->lcb_, LCB_CALLBACK_REMOVE, reinterpret_cast<lcb_RESPCALLBACK>(remove_callback));
    lcb_install_callback(bucket_->lcb_, LCB_CALLBACK_SDLOOKUP, reinterpret_cast<lcb_RESPCALLBACK>(subdoc_callback));
    lcb_install_callback(bucket_->lcb_, LCB_CALLBACK_SDMUTATE, reinterpret_cast<lcb_RESPCALLBACK>(subdoc_callback));
    const char* tmp;
    lcb_cntl(bucket_->lcb_, LCB_CNTL_GET, LCB_CNTL_BUCKETNAME, &tmp);
    bucket_name_ = std::string(tmp);
}
