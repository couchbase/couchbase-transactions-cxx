#pragma once

#include <string>
#include <vector>

#include <couchbase/client/bucket.hxx>
#include <couchbase/client/lookup_in_spec.hxx>
#include <couchbase/client/mutate_in_spec.hxx>
#include <couchbase/client/result.hxx>

namespace couchbase
{

enum class durability_level { none, majority, majority_and_persist_to_active, persist_to_majority };
enum class store_operation { upsert, insert, replace };

result
store_impl(collection* coll, store_operation op, const std::string& id, const std::string& payload, uint64_t cas, durability_level level);

class collection
{
    friend class bucket;

  private:
    std::string scope_;
    std::string name_;
    std::shared_ptr<bucket> bucket_;
    std::string bucket_name_;

    friend result store_impl(collection* coll,
                             store_operation op,
                             const std::string& id,
                             const std::string& payload,
                             uint64_t cas,
                             durability_level level);

    lcb_st* lcb()
    {
        return bucket_->lcb_;
    }

    template<typename Content>
    result store(store_operation operation, const std::string& id, const Content& value, uint64_t cas, durability_level level)
    {
        nlohmann::json j = value;
        std::string payload = j.dump();
        return store_impl(this, operation, id, payload, cas, level);
    }

  public:
    explicit collection(std::shared_ptr<bucket> bucket, std::string scope, std::string name);

    result get(const std::string& id, uint32_t expiry = 0);

    template<typename Content>
    result upsert(const std::string& id, const Content& value, uint64_t cas = 0, durability_level level = durability_level::none)
    {
        return store(store_operation::upsert, id, value, cas, level);
    }

    template<typename Content>
    result insert(const std::string& id, const Content& value, durability_level level = durability_level::none)
    {
        return store(store_operation::insert, id, value, 0, level);
    }

    template<typename Content>
    result replace(const std::string& id, const Content& value, uint64_t cas, durability_level level = durability_level::none)
    {
        return store(store_operation::replace, id, value, cas, level);
    }

    result remove(const std::string& id, uint64_t cas = 0, durability_level level = durability_level::none);

    result mutate_in(const std::string& id, std::vector<mutate_in_spec> specs, durability_level level = durability_level::none);

    result lookup_in(const std::string& id, std::vector<lookup_in_spec> specs);

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
