#pragma once

#include <memory>
struct lcb_st;

namespace couchbase
{
class collection;

/**
 * Couchbase bucket.
 *
 * Exposes bucket-level operations and collections accessors.
 */
class bucket : public std::enable_shared_from_this<bucket>
{
    friend class collection;

  private:
    lcb_st* lcb_;
    bucket(lcb_st* instance, const std::string& name);

  public:
    // this insures that a shared_ptr<bucket> exists before we ever try to
    // do a shared_from_this() call with it later
    static std::shared_ptr<bucket> create(lcb_st* instance, const std::string& name) {
        return std::shared_ptr<bucket>(new bucket(instance, name));
    }
    std::shared_ptr<class collection> default_collection();
    std::shared_ptr<class collection> collection(const std::string &name);
};
} // namespace couchbase
