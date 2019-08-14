#pragma once

#include <string>
#include <vector>

#include <libcouchbase/result.hxx>
#include <libcouchbase/mutate_in_spec.hxx>
#include <libcouchbase/lookup_in_spec.hxx>

namespace couchbase
{

class bucket;

class collection
{
    friend class bucket;

  private:
    std::string scope_;
    std::string name_;
    bucket *bucket_;
    std::string bucket_name_;

    explicit collection(bucket *bucket, std::string scope, std::string name);

    result store(lcb_STORE_OPERATION operation, const std::string &id, const std::string &value, uint64_t cas, lcb_DURABILITY_LEVEL level);

  public:
    result get(const std::string &id, uint32_t expiry = 0);
    result upsert(const std::string &id, const std::string &value, uint64_t cas = 0, lcb_DURABILITY_LEVEL level = LCB_DURABILITYLEVEL_NONE);
    result insert(const std::string &id, const std::string &value, lcb_DURABILITY_LEVEL level = LCB_DURABILITYLEVEL_NONE);
    result replace(const std::string &id, const std::string &value, uint64_t cas, lcb_DURABILITY_LEVEL level = LCB_DURABILITYLEVEL_NONE);
    result remove(const std::string &id, uint64_t cas, lcb_DURABILITY_LEVEL level = LCB_DURABILITYLEVEL_NONE);

    result mutate_in(const std::string &id, const std::vector<mutate_in_spec> &specs, lcb_DURABILITY_LEVEL level = LCB_DURABILITYLEVEL_NONE);
    result lookup_in(const std::string &id, const std::vector<lookup_in_spec> &specs);

    const std::string &name() const;
    const std::string &scope() const;
    const std::string &bucket_name() const;
};
} // namespace couchbase
