#pragma once

#include <string>
#include <vector>

#include <couchbase/client/bucket.hxx>
#include <couchbase/client/lookup_in_spec.hxx>
#include <couchbase/client/mutate_in_spec.hxx>
#include <couchbase/client/result.hxx>

namespace couchbase
{

class collection
{
    friend class bucket;

  private:
    std::string scope_;
    std::string name_;
    std::shared_ptr<bucket> bucket_;
    std::string bucket_name_;

    explicit collection(std::shared_ptr<bucket> bucket, std::string scope, std::string name);

    template<typename Content>
    result store(lcb_STORE_OPERATION operation, const std::string& id, const Content& value, uint64_t cas, lcb_DURABILITY_LEVEL level)
    {
        lcb_CMDSTORE* cmd;
        lcb_cmdstore_create(&cmd, operation);
        lcb_cmdstore_key(cmd, id.data(), id.size());
        nlohmann::json j = value;
        std::string payload = j.dump();
        lcb_cmdstore_value(cmd, payload.data(), payload.size());
        lcb_cmdstore_cas(cmd, cas);
        lcb_cmdstore_collection(cmd, scope_.data(), scope_.size(), name_.data(), name_.size());
        lcb_cmdstore_durability(cmd, level);
        lcb_STATUS rc;
        result res;
        rc = lcb_store(bucket_->lcb_, reinterpret_cast<void*>(&res), cmd);
        lcb_cmdstore_destroy(cmd);
        if (rc != LCB_SUCCESS) {
            throw std::runtime_error(std::string("failed to store (sched) document: ") + lcb_strerror_short(rc));
        }
        lcb_wait(bucket_->lcb_, LCB_WAIT_DEFAULT);
        return res;
    }

  public:
    result get(const std::string& id, uint32_t expiry = 0)
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
        rc = lcb_get(bucket_->lcb_, reinterpret_cast<void*>(&res), cmd);
        lcb_cmdget_destroy(cmd);
        if (rc != LCB_SUCCESS) {
            throw std::runtime_error(std::string("failed to get (sched) document: ") + lcb_strerror_short(rc));
        }
        lcb_wait(bucket_->lcb_, LCB_WAIT_DEFAULT);
        return res;
    }

    template<typename Content>
    result upsert(const std::string& id, const Content& value, uint64_t cas = 0, lcb_DURABILITY_LEVEL level = LCB_DURABILITYLEVEL_NONE)
    {
        return store(LCB_STORE_UPSERT, id, value, cas, level);
    }

    template<typename Content>
    result insert(const std::string& id, const Content& value, lcb_DURABILITY_LEVEL level = LCB_DURABILITYLEVEL_NONE)
    {
        return store(LCB_STORE_INSERT, id, value, 0, level);
    }

    template<typename Content>
    result replace(const std::string& id, const Content& value, uint64_t cas, lcb_DURABILITY_LEVEL level = LCB_DURABILITYLEVEL_NONE)
    {
        return store(LCB_STORE_REPLACE, id, value, cas, level);
    }

    result remove(const std::string& id, uint64_t cas = 0, lcb_DURABILITY_LEVEL level = LCB_DURABILITYLEVEL_NONE)
    {
        lcb_CMDREMOVE* cmd;
        lcb_cmdremove_create(&cmd);
        lcb_cmdremove_key(cmd, id.data(), id.size());
        lcb_cmdremove_cas(cmd, cas);
        lcb_cmdremove_collection(cmd, scope_.data(), scope_.size(), name_.data(), name_.size());
        lcb_cmdremove_durability(cmd, level);
        lcb_STATUS rc;
        result res;
        rc = lcb_remove(bucket_->lcb_, reinterpret_cast<void*>(&res), cmd);
        lcb_cmdremove_destroy(cmd);
        if (rc != LCB_SUCCESS) {
            throw std::runtime_error(std::string("failed to remove (sched) document: ") + lcb_strerror_short(rc));
        }
        lcb_wait(bucket_->lcb_, LCB_WAIT_DEFAULT);
        return res;
    }

    result mutate_in(const std::string& id, std::vector<mutate_in_spec> specs, lcb_DURABILITY_LEVEL level = LCB_DURABILITYLEVEL_NONE)
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
                    lcb_subdocspecs_dict_upsert(ops, idx++, spec.flags_, nullptr, 0, spec.value_.data(), spec.value_.size());
                    lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_UPSERT);
                    break;
                case mutate_in_spec_type::MUTATE_IN_FULLDOC_INSERT:
                    lcb_subdocspecs_dict_upsert(ops, idx++, spec.flags_, nullptr, 0, spec.value_.data(), spec.value_.size());
                    lcb_cmdsubdoc_store_semantics(cmd, LCB_SUBDOC_STORE_INSERT);
                    break;
                case mutate_in_spec_type::REMOVE:
                    lcb_subdocspecs_remove(ops, idx++, spec.flags_, spec.path_.data(), spec.path_.size());
                    break;
            }
        }
        lcb_cmdsubdoc_specs(cmd, ops);
        lcb_cmdsubdoc_durability(cmd, level);
        lcb_STATUS rc;
        result res;
        rc = lcb_subdoc(bucket_->lcb_, reinterpret_cast<void*>(&res), cmd);
        lcb_cmdsubdoc_destroy(cmd);
        lcb_subdocspecs_destroy(ops);
        if (rc != LCB_SUCCESS) {
            throw std::runtime_error(std::string("failed to mutate (sched) sub-document: ") + lcb_strerror_short(rc));
        }
        lcb_wait(bucket_->lcb_, LCB_WAIT_DEFAULT);
        return res;
    }

    result lookup_in(const std::string& id, std::vector<lookup_in_spec> specs)
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
        rc = lcb_subdoc(bucket_->lcb_, reinterpret_cast<void*>(&res), cmd);
        lcb_cmdsubdoc_destroy(cmd);
        lcb_subdocspecs_destroy(ops);
        if (rc != LCB_SUCCESS) {
            throw std::runtime_error(std::string("failed to lookup (sched) sub-document: ") + lcb_strerror_short(rc));
        }
        lcb_wait(bucket_->lcb_, LCB_WAIT_DEFAULT);
        return res;
    }

    [[nodiscard]] const std::string& name() const
    {
        return name_;
    }

    [[nodiscard]] const std::string& scope() const
    {
        return scope_;
    }

    [[nodiscard]] const std::string& bucket_name() const
    {
        return bucket_name_;
    }
};
} // namespace couchbase
