#pragma once

#include <string>
#include <vector>

#include <libcouchbase/result.hxx>
#include <libcouchbase/mutate_in_spec.hxx>
#include <libcouchbase/lookup_in_spec.hxx>

namespace couchbase
{

class Bucket;

class Collection
{
    friend class Bucket;

  private:
    std::string scope_;
    std::string name_;
    Bucket *bucket_;
    std::string bucket_name_;

    explicit Collection(Bucket *bucket, const std::string &scope, const std::string &name);

    Result store(lcb_STORE_OPERATION operation, const std::string &id, const std::string &value, uint64_t cas);

  public:
    Result get(const std::string &id);
    Result upsert(const std::string &id, const std::string &value, uint64_t cas = 0);
    Result insert(const std::string &id, const std::string &value);
    Result replace(const std::string &id, const std::string &value, uint64_t cas);
    Result remove(const std::string &id, uint64_t cas);

    Result mutate_in(const std::string &id, const std::vector< MutateInSpec > &specs);
    Result lookup_in(const std::string &id, const std::vector< LookupInSpec > &specs);

    const std::string &name() const;
    const std::string &scope() const;
    const std::string &bucket_name() const;
};
} // namespace couchbase
