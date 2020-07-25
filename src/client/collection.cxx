#include <utility>
#include <cassert>
#include <spdlog/spdlog.h>

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
    spdlog::trace(res->to_string());
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
    spdlog::trace(res->to_string());
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
    spdlog::trace(res->to_string());
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
    spdlog::info("subdoc_callback: got {} results for {}", len, res->key);
    for (size_t idx = 0; idx < len; idx++) {
        data = nullptr;
        ndata = 0;
        lcb_respsubdoc_result_value(resp, idx, &data, &ndata);
        spdlog::info("got {} length data for {}", ndata, idx);
        auto itr = res->values.begin() + idx;
        if (data) {
            res->values.emplace(itr, nlohmann::json::parse(data, data + ndata));
        } else {
            spdlog::info("writing null for results[{}]", idx);
            res->values.emplace(itr, boost::none);
        }
    }
    spdlog::trace(res->to_string());
}
}

cb::collection::collection(std::shared_ptr<bucket> bucket, std::string scope, std::string name)
  : bucket_(std::move(bucket))
  , scope_(std::move(scope))
  , name_(std::move(name))
{
    assert(bucket_->lcb_);
    lcb_install_callback(bucket_->lcb_, LCB_CALLBACK_STORE, reinterpret_cast<lcb_RESPCALLBACK>(store_callback));
    lcb_install_callback(bucket_->lcb_, LCB_CALLBACK_GET, reinterpret_cast<lcb_RESPCALLBACK>(get_callback));
    lcb_install_callback(bucket_->lcb_, LCB_CALLBACK_REMOVE, reinterpret_cast<lcb_RESPCALLBACK>(remove_callback));
    lcb_install_callback(bucket_->lcb_, LCB_CALLBACK_SDLOOKUP, reinterpret_cast<lcb_RESPCALLBACK>(subdoc_callback));
    lcb_install_callback(bucket_->lcb_, LCB_CALLBACK_SDMUTATE, reinterpret_cast<lcb_RESPCALLBACK>(subdoc_callback));
    const char* tmp;
    lcb_cntl(bucket_->lcb_, LCB_CNTL_GET, LCB_CNTL_BUCKETNAME, &tmp);
    bucket_name_ = std::string(tmp);
}

static lcb_DURABILITY_LEVEL
convert_durability(couchbase::durability_level level)
{
    switch (level) {
        case couchbase::durability_level::none:
            return LCB_DURABILITYLEVEL_NONE;
        case couchbase::durability_level::majority:
            return LCB_DURABILITYLEVEL_MAJORITY;
        case couchbase::durability_level::majority_and_persist_to_active:
            return LCB_DURABILITYLEVEL_MAJORITY_AND_PERSIST_TO_ACTIVE;
        case couchbase::durability_level::persist_to_majority:
            return LCB_DURABILITYLEVEL_PERSIST_TO_MAJORITY;
    }
    return LCB_DURABILITYLEVEL_NONE;
}

couchbase::result
couchbase::store_impl(couchbase::collection* collection,
                      couchbase::store_operation op,
                      const std::string& id,
                      const std::string& payload,
                      uint64_t cas,
                      couchbase::durability_level level)
{
    lcb_CMDSTORE* cmd = nullptr;
    lcb_STORE_OPERATION storeop = LCB_STORE_UPSERT;
    switch (op) {
        case couchbase::store_operation::upsert:
            storeop = LCB_STORE_UPSERT;
            break;
        case couchbase::store_operation::insert:
            storeop = LCB_STORE_INSERT;
            break;
        case couchbase::store_operation::replace:
            storeop = LCB_STORE_REPLACE;
            break;
    }
    lcb_cmdstore_create(&cmd, storeop);
    lcb_cmdstore_key(cmd, id.data(), id.size());
    lcb_cmdstore_value(cmd, payload.data(), payload.size());
    lcb_cmdstore_cas(cmd, cas);
    lcb_cmdstore_collection(cmd, collection->scope_.data(), collection->scope_.size(), collection->name_.data(), collection->name_.size());
    lcb_cmdstore_durability(cmd, convert_durability(level));
    result res;
    assert(collection->lcb());
    lcb_STATUS rc = lcb_store(collection->lcb(), reinterpret_cast<void*>(&res), cmd);
    lcb_cmdstore_destroy(cmd);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to store (sched) document: ") + lcb_strerror_short(rc));
    }
    lcb_wait(collection->lcb(), LCB_WAIT_DEFAULT);
    return res;
}

couchbase::result
couchbase::collection::get(const std::string& id, uint32_t expiry)
{
    lcb_CMDGET* cmd;
    lcb_cmdget_create(&cmd);
    lcb_cmdget_key(cmd, id.data(), id.size());
    lcb_cmdget_collection(cmd, scope_.data(), scope_.size(), name_.data(), name_.size());
    if (expiry) {
        lcb_cmdget_expiry(cmd, expiry);
    }
    lcb_STATUS rc;
    result res;
    assert(bucket_->lcb_);
    rc = lcb_get(bucket_->lcb_, reinterpret_cast<void*>(&res), cmd);
    lcb_cmdget_destroy(cmd);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to get (sched) document: ") + lcb_strerror_short(rc));
    }
    lcb_wait(bucket_->lcb_, LCB_WAIT_DEFAULT);
    return res;
}

couchbase::result
couchbase::collection::remove(const std::string& id, uint64_t cas, couchbase::durability_level level)
{
    lcb_CMDREMOVE* cmd;
    lcb_cmdremove_create(&cmd);
    lcb_cmdremove_key(cmd, id.data(), id.size());
    lcb_cmdremove_cas(cmd, cas);
    lcb_cmdremove_collection(cmd, scope_.data(), scope_.size(), name_.data(), name_.size());
    lcb_cmdremove_durability(cmd, convert_durability(level));
    lcb_STATUS rc;
    result res;
    assert(bucket_->lcb_);
    rc = lcb_remove(bucket_->lcb_, reinterpret_cast<void*>(&res), cmd);
    lcb_cmdremove_destroy(cmd);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to remove (sched) document: ") + lcb_strerror_short(rc));
    }
    lcb_wait(bucket_->lcb_, LCB_WAIT_DEFAULT);
    return res;
}

couchbase::result
couchbase::collection::mutate_in(const std::string& id, std::vector<mutate_in_spec> specs, couchbase::durability_level level)
{
    lcb_CMDSUBDOC* cmd;
    lcb_cmdsubdoc_create(&cmd);
    lcb_cmdsubdoc_key(cmd, id.data(), id.size());
    lcb_cmdsubdoc_collection(cmd, scope_.data(), scope_.size(), name_.data(), name_.size());
    lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_UPSERT);

    lcb_SUBDOCSPECS* ops;
    lcb_subdocspecs_create(&ops, specs.size());
    size_t idx = 0;
    for (const auto& spec : specs) {
        switch (spec.type_) {
            case mutate_in_spec_type::MUTATE_IN_UPSERT:
                lcb_subdocspecs_dict_upsert(
                  ops, idx++, spec.flags_, spec.path_.data(), spec.path_.size(), spec.value_.data(), spec.value_.size());
                break;
            case mutate_in_spec_type::MUTATE_IN_INSERT:
                lcb_subdocspecs_dict_add(
                  ops, idx++, spec.flags_, spec.path_.data(), spec.path_.size(), spec.value_.data(), spec.value_.size());
                break;
            case mutate_in_spec_type::MUTATE_IN_FULLDOC_UPSERT:
                lcb_subdocspecs_replace(ops, idx++, spec.flags_, nullptr, 0, spec.value_.data(), spec.value_.size());
                lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_UPSERT);
                break;
            case mutate_in_spec_type::MUTATE_IN_FULLDOC_INSERT:
                lcb_subdocspecs_replace(ops, idx++, spec.flags_, nullptr, 0, spec.value_.data(), spec.value_.size());
                lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_INSERT);
                break;
            case mutate_in_spec_type::REMOVE:
                lcb_subdocspecs_remove(ops, idx++, spec.flags_, spec.path_.data(), spec.path_.size());
                break;
        }
    }
    lcb_cmdsubdoc_specs(cmd, ops);
    lcb_cmdsubdoc_durability(cmd, convert_durability(level));
    lcb_STATUS rc;
    result res;
    assert(bucket_->lcb_);
    rc = lcb_subdoc(bucket_->lcb_, reinterpret_cast<void*>(&res), cmd);
    lcb_cmdsubdoc_destroy(cmd);
    lcb_subdocspecs_destroy(ops);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to mutate (sched) sub-document: ") + lcb_strerror_short(rc));
    }
    lcb_wait(bucket_->lcb_, LCB_WAIT_DEFAULT);
    return res;
}

couchbase::result
couchbase::collection::lookup_in(const std::string& id, std::vector<lookup_in_spec> specs)
{
    lcb_CMDSUBDOC* cmd;
    lcb_cmdsubdoc_create(&cmd);
    lcb_cmdsubdoc_key(cmd, id.data(), id.size());
    lcb_cmdsubdoc_collection(cmd, scope_.data(), scope_.size(), name_.data(), name_.size());

    lcb_SUBDOCSPECS* ops;
    lcb_subdocspecs_create(&ops, specs.size());
    size_t idx = 0;
    for (const auto& spec : specs) {
        switch (spec.type_) {
            case lookup_in_spec_type::LOOKUP_IN_GET:
                lcb_subdocspecs_get(ops, idx++, spec.flags_, spec.path_.data(), spec.path_.size());
                break;
            case lookup_in_spec_type::LOOKUP_IN_FULLDOC_GET:
                lcb_subdocspecs_get(ops, idx++, spec.flags_, nullptr, 0);
                break;
        }
    }
    lcb_cmdsubdoc_specs(cmd, ops);
    lcb_STATUS rc;
    result res;
    assert(bucket_->lcb_);
    rc = lcb_subdoc(bucket_->lcb_, reinterpret_cast<void*>(&res), cmd);
    lcb_cmdsubdoc_destroy(cmd);
    lcb_subdocspecs_destroy(ops);
    if (rc != LCB_SUCCESS) {
        throw std::runtime_error(std::string("failed to lookup (sched) sub-document: ") + lcb_strerror_short(rc));
    }
    lcb_wait(bucket_->lcb_, LCB_WAIT_DEFAULT);
    return res;
}
