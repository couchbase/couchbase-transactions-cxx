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

#include <cassert>
#include <utility>

#include "logging.hxx"
#include "pool.hxx"

#include <couchbase/client/bucket.hxx>
#include <couchbase/client/collection.hxx>
#include <couchbase/client/lookup_in_spec.hxx>
#include <libcouchbase/couchbase.h>
#include <spdlog/fmt/ostr.h>

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
    res->key = std::move(std::string(data, ndata));
    cb::client_log->trace("store_callback returning {}", *res);
}

static void
get_callback(lcb_INSTANCE* lcb, int, const lcb_RESPGET* resp)
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
        res->key = std::move(std::string(data, ndata));
        lcb_respget_value(resp, &data, &ndata);
        res->raw_value.assign(data, ndata);
    }
    cb::client_log->trace("{}: get_callback returning {}", (void*)lcb, *res);
}

static void
exists_callback(lcb_INSTANCE* lcb, int, const lcb_RESPEXISTS* resp)
{
    cb::result* res = nullptr;
    lcb_respexists_cookie(resp, reinterpret_cast<void**>(&res));
    res->rc = lcb_respexists_status(resp);
    if (res->rc == LCB_SUCCESS) {
        if (lcb_respexists_is_found(resp)) {
            lcb_respexists_cas(resp, &res->cas);
            res->raw_value.assign("true");
        } else {
            res->raw_value.assign("false");
        }
    } else {
        res->raw_value.assign("false");
    }
    cb::client_log->trace("{}: exists_callback returning {}", (void*)lcb, *res);
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
    res->key = std::move(std::string(data, ndata));
    cb::client_log->trace("remove_callback returning {}", *res);
}

static void
subdoc_callback(lcb_INSTANCE* instance, int, const lcb_RESPSUBDOC* resp)
{
    cb::result* res = nullptr;
    lcb_respsubdoc_cookie(resp, reinterpret_cast<void**>(&res));
    res->rc = lcb_respsubdoc_status(resp);
    lcb_respsubdoc_cas(resp, &res->cas);
    const char* data = nullptr;
    size_t ndata = 0;
    lcb_respsubdoc_key(resp, &data, &ndata);
    res->key = std::move(std::string(data, ndata));

    size_t len = lcb_respsubdoc_result_size(resp);
    res->values.reserve(len);
    for (size_t idx = 0; idx < len; idx++) {
        data = nullptr;
        ndata = 0;
        lcb_STATUS status = lcb_respsubdoc_result_status(resp, idx);
        lcb_respsubdoc_result_value(resp, idx, &data, &ndata);
        auto itr = res->values.begin() + idx;
        if (data) {
            res->values.emplace(itr, std::string(data, ndata), status);
        } else {
            res->values.emplace(itr, status);
        }
    }
    if (len > 0) {
        res->is_deleted = lcb_respsubdoc_is_deleted(resp);
    }
}
}

void
cb::collection::install_callbacks(lcb_st* lcb)
{
    lcb_install_callback(lcb, LCB_CALLBACK_STORE, reinterpret_cast<lcb_RESPCALLBACK>(store_callback));
    lcb_install_callback(lcb, LCB_CALLBACK_GET, reinterpret_cast<lcb_RESPCALLBACK>(get_callback));
    lcb_install_callback(lcb, LCB_CALLBACK_EXISTS, reinterpret_cast<lcb_RESPCALLBACK>(exists_callback));
    lcb_install_callback(lcb, LCB_CALLBACK_REMOVE, reinterpret_cast<lcb_RESPCALLBACK>(remove_callback));
    lcb_install_callback(lcb, LCB_CALLBACK_SDLOOKUP, reinterpret_cast<lcb_RESPCALLBACK>(subdoc_callback));
    lcb_install_callback(lcb, LCB_CALLBACK_SDMUTATE, reinterpret_cast<lcb_RESPCALLBACK>(subdoc_callback));
}

cb::collection::collection(std::shared_ptr<cb::bucket> bucket, std::string scope, std::string name, std::chrono::microseconds kv_timeout)
  : bucket_(std::move(bucket))
  , scope_(std::move(scope))
  , name_(std::move(name))
  , kv_timeout_(std::move(kv_timeout))
{
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

static lcb_SUBDOC_STORE_SEMANTICS
convert_semantics(couchbase::subdoc_store_semantics semantics)
{
    switch (semantics) {
        case couchbase::subdoc_store_semantics::upsert:
            return LCB_SUBDOC_STORE_UPSERT;
        case couchbase::subdoc_store_semantics::insert:
            return LCB_SUBDOC_STORE_INSERT;
        case couchbase::subdoc_store_semantics::replace:
            return LCB_SUBDOC_STORE_REPLACE;
        default:
            throw std::runtime_error("Unknown subdoc store semantics specified");
    }
}

static lcb_STORE_OPERATION
convert_operation(couchbase::store_operation op)
{
    switch (op) {
        case couchbase::store_operation::upsert:
            return LCB_STORE_UPSERT;
        case couchbase::store_operation::insert:
            return LCB_STORE_INSERT;
        case couchbase::store_operation::replace:
            return LCB_STORE_REPLACE;
        default:
            throw std::runtime_error("Unknown store operation specified");
    }
}
couchbase::result
couchbase::store_impl(couchbase::collection* collection,
                      couchbase::store_operation op,
                      const std::string& id,
                      const std::string& payload,
                      uint64_t cas,
                      durability_level level,
                      std::chrono::microseconds timeout)
{
    return collection->instance_pool()->wrap_access<result>([&](lcb_st* lcb) -> result {
        lcb_CMDSTORE* cmd = nullptr;
        lcb_STORE_OPERATION storeop = convert_operation(op);
        lcb_cmdstore_create(&cmd, storeop);
        lcb_cmdstore_key(cmd, id.data(), id.size());
        lcb_cmdstore_value(cmd, payload.data(), payload.size());
        lcb_cmdstore_cas(cmd, cas);
        if (collection->set_collection()) {
            lcb_cmdstore_collection(
              cmd, collection->scope_.data(), collection->scope_.size(), collection->name_.data(), collection->name_.size());
        }
        lcb_cmdstore_durability(cmd, convert_durability(level));
        lcb_cmdstore_timeout(cmd, timeout.count());
        result res;
        lcb_STATUS rc = lcb_store(lcb, reinterpret_cast<void*>(&res), cmd);
        lcb_cmdstore_destroy(cmd);
        if (rc != LCB_SUCCESS) {
            throw std::runtime_error(std::string("failed to store (sched) document: ") + lcb_strerror_short(rc));
        }
        lcb_wait(lcb, LCB_WAIT_DEFAULT);
        return res;
    });
}

couchbase::result
couchbase::collection::wrap_call_for_retry(std::chrono::microseconds initial_timeout,
                                           std::function<result(std::chrono::microseconds timeout)> fn)
{
    int retries = 0;
    auto end = std::chrono::steady_clock::now() + initial_timeout;
    auto timeout = initial_timeout;
    auto delay = std::chrono::milliseconds(1);
    while (timeout.count() > 0) {
        auto res = fn(timeout);
        switch (res.rc) {
            case LCB_ERR_KVENGINE_INVALID_PACKET:
            case LCB_ERR_KVENGINE_UNKNOWN_ERROR:
                // remove these when CCBC-1300 is fixed
                cb::client_log->trace("got {}, retrying due to CCBC-1300", res.rc);
                cb::client_log->trace("retry #{}, sleeping {}ms before retrying...", retries, delay.count());
                std::this_thread::sleep_for(delay);
                retries++;
                if (retries < 7) {
                    // 2^7 = 128 -- that seems like a reasonable random cap.
                    delay = 2 * delay;
                }
                timeout = std::chrono::duration_cast<std::chrono::microseconds>(end - std::chrono::steady_clock::now());
                break;
            default:
                return res;
        }
    }
    // if we are here, we timed out after retrying, so...
    result r;
    r.rc = LCB_ERR_TIMEOUT;
    return r;
}

couchbase::result
couchbase::collection::get(const std::string& id, const get_options& opts)
{
    auto init_timeout = (opts.timeout() ? *opts.timeout() : default_kv_timeout());
    return wrap_call_for_retry(init_timeout, [&](std::chrono::microseconds timeout) -> result {
        lcb_CMDGET* cmd;
        lcb_cmdget_create(&cmd);
        lcb_cmdget_key(cmd, id.data(), id.size());
        if (set_collection()) {
            lcb_cmdget_collection(cmd, scope_.data(), scope_.size(), name_.data(), name_.size());
        }
        if (opts.expiry()) {
            // does a 'get and touch'
            lcb_cmdget_expiry(cmd, *opts.expiry());
        }
        lcb_cmdget_timeout(cmd, timeout.count());
        lcb_STATUS rc;
        return bucket_.lock()->instance_pool_->wrap_access<couchbase::result>([&](lcb_st* lcb) -> couchbase::result {
            result res;
            rc = lcb_get(lcb, reinterpret_cast<void*>(&res), cmd);
            lcb_cmdget_destroy(cmd);
            if (rc != LCB_SUCCESS) {
                throw std::runtime_error(std::string("failed to get (sched) document: ") + lcb_strerror_short(rc));
            }
            lcb_wait(lcb, LCB_WAIT_DEFAULT);
            return res;
        });
    });
}

couchbase::result
couchbase::collection::exists(const std::string& id, const exists_options& opts)
{
    auto init_timeout = (opts.timeout() ? *opts.timeout() : default_kv_timeout());
    return wrap_call_for_retry(init_timeout, [&](std::chrono::microseconds timeout) -> result {
        lcb_CMDEXISTS* cmd;
        lcb_cmdexists_create(&cmd);
        lcb_cmdexists_key(cmd, id.data(), id.size());
        lcb_cmdexists_timeout(cmd, timeout.count());
        if (set_collection()) {
            lcb_cmdexists_collection(cmd, scope_.data(), scope_.size(), name_.data(), name_.size());
        }
        lcb_STATUS rc;
        return bucket_.lock()->instance_pool_->wrap_access<couchbase::result>([&](lcb_st* lcb) -> couchbase::result {
            result res;
            rc = lcb_exists(lcb, reinterpret_cast<void*>(&res), cmd);
            lcb_cmdexists_destroy(cmd);
            if (rc != LCB_SUCCESS) {
                throw std::runtime_error(std::string("failed to exists (sched) document: ") + lcb_strerror_short(rc));
            }
            lcb_wait(lcb, LCB_WAIT_DEFAULT);
            return res;
        });
    });
}

couchbase::result
couchbase::collection::remove(const std::string& id, const remove_options& opts)
{
    auto init_timeout = (opts.timeout() ? *opts.timeout() : default_kv_timeout());
    return wrap_call_for_retry(init_timeout, [&](std::chrono::microseconds timeout) -> result {
        lcb_CMDREMOVE* cmd;
        lcb_cmdremove_create(&cmd);
        lcb_cmdremove_timeout(cmd, timeout.count());
        lcb_cmdremove_create(&cmd);
        lcb_cmdremove_key(cmd, id.data(), id.size());
        if (opts.cas()) {
            lcb_cmdremove_cas(cmd, *opts.cas());
        }
        if (set_collection()) {
            lcb_cmdremove_collection(cmd, scope_.data(), scope_.size(), name_.data(), name_.size());
        }
        if (opts.durability()) {
            lcb_cmdremove_durability(cmd, convert_durability(*opts.durability()));
        }
        lcb_STATUS rc;
        return bucket_.lock()->instance_pool_->wrap_access<couchbase::result>([&](lcb_st* lcb) -> couchbase::result {
            result res;
            rc = lcb_remove(lcb, reinterpret_cast<void*>(&res), cmd);
            lcb_cmdremove_destroy(cmd);
            if (rc != LCB_SUCCESS) {
                throw std::runtime_error(std::string("failed to remove (sched) document: ") + lcb_strerror_short(rc));
            }
            lcb_wait(lcb, LCB_WAIT_DEFAULT);
            return res;
        });
    });
}

couchbase::result
couchbase::collection::mutate_in(const std::string& id, std::vector<mutate_in_spec> specs, const mutate_in_options& opts)
{
    auto init_timeout = (opts.timeout() ? *opts.timeout() : default_kv_timeout());
    return wrap_call_for_retry(init_timeout, [&](std::chrono::microseconds timeout) -> result {
        lcb_CMDSUBDOC* cmd;
        lcb_cmdsubdoc_create(&cmd);
        lcb_cmdsubdoc_timeout(cmd, timeout.count());
        lcb_cmdsubdoc_key(cmd, id.data(), id.size());
        if (set_collection()) {
            lcb_cmdsubdoc_collection(cmd, scope_.data(), scope_.size(), name_.data(), name_.size());
        }
        if (opts.cas()) {
            lcb_cmdsubdoc_cas(cmd, *opts.cas());
        }

        if (opts.create_as_deleted()) {
            lcb_cmdsubdoc_create_as_deleted(cmd, true);
            if (opts.cas() && *opts.cas() > 0) {
                lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_UPSERT);
            } else {
                lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_INSERT);
            }
        }
        if (opts.access_deleted()) {
            lcb_cmdsubdoc_access_deleted(cmd, true);
        }
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
                    if (!opts.create_as_deleted()) {
                        lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_UPSERT);
                    }
                    break;
                case mutate_in_spec_type::MUTATE_IN_FULLDOC_INSERT:
                    lcb_subdocspecs_replace(ops, idx++, spec.flags_, nullptr, 0, spec.value_.data(), spec.value_.size());
                    if (!opts.create_as_deleted()) {
                        lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_INSERT);
                    }
                    break;
                case mutate_in_spec_type::REMOVE:
                    lcb_subdocspecs_remove(ops, idx++, spec.flags_, spec.path_.data(), spec.path_.size());
                    break;
            }
        }
        lcb_cmdsubdoc_specs(cmd, ops);
        if (opts.durability()) {
            lcb_cmdsubdoc_durability(cmd, convert_durability(*opts.durability()));
        }
        if (opts.store_semantics()) {
            lcb_cmdsubdoc_store_semantics(cmd, convert_semantics(*opts.store_semantics()));
        }
        lcb_STATUS rc;
        return bucket_.lock()->instance_pool_->wrap_access<couchbase::result>([&](lcb_st* lcb) -> couchbase::result {
            result res;
            rc = lcb_subdoc(lcb, reinterpret_cast<void*>(&res), cmd);
            lcb_cmdsubdoc_destroy(cmd);
            lcb_subdocspecs_destroy(ops);
            if (rc != LCB_SUCCESS) {
                throw std::runtime_error(std::string("failed to mutate (sched) sub-document: ") + lcb_strerror_short(rc));
            }
            lcb_wait(lcb, LCB_WAIT_DEFAULT);
            // HACK!  LCB returns LCB_ERR_DOCUMENT_EXISTS when it should return
            // LCB_ERR_CAS_MISMATCH, for mutate_in.  For now, hack in a fix till LCB
            // is fixed (CCBC-1323).  However, if the semantics are insert, then the
            // error code is correct
            if (res.rc == LCB_ERR_DOCUMENT_EXISTS &&
                (!opts.store_semantics() || !(opts.store_semantics() == couchbase::subdoc_store_semantics::insert))) {
                res.rc = LCB_ERR_CAS_MISMATCH;
            }
            res.ignore_subdoc_errors = false;
            cb::client_log->trace("mutate_in returning {}", res);
            return res;
        });
    });
}

couchbase::result
couchbase::collection::lookup_in(const std::string& id, std::vector<lookup_in_spec> specs, const lookup_in_options& opts)
{
    auto init_timeout = (opts.timeout() ? *opts.timeout() : default_kv_timeout());
    return wrap_call_for_retry(init_timeout, [&](std::chrono::microseconds timeout) -> result {
        lcb_CMDSUBDOC* cmd;
        lcb_cmdsubdoc_create(&cmd);
        lcb_cmdsubdoc_timeout(cmd, timeout.count());
        lcb_cmdsubdoc_key(cmd, id.data(), id.size());
        if (set_collection()) {
            lcb_cmdsubdoc_collection(cmd, scope_.data(), scope_.size(), name_.data(), name_.size());
        }
        if (opts.access_deleted()) {
            lcb_cmdsubdoc_access_deleted(cmd, true);
        }
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
        return bucket_.lock()->instance_pool_->wrap_access<couchbase::result>([&](lcb_st* lcb) -> couchbase::result {
            result res;
            rc = lcb_subdoc(lcb, reinterpret_cast<void*>(&res), cmd);
            lcb_cmdsubdoc_destroy(cmd);
            lcb_subdocspecs_destroy(ops);
            if (rc != LCB_SUCCESS) {
                throw std::runtime_error(std::string("failed to lookup (sched) sub-document: ") + lcb_strerror_short(rc));
            }
            lcb_wait(lcb, LCB_WAIT_DEFAULT);
            res.ignore_subdoc_errors = true;
            cb::client_log->trace("lookup_in returning {}", res);
            return res;
        });
    });
}
