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

  public:
    bucket(lcb_st* instance, const std::string& name);
    collection* default_collection();
};
} // namespace couchbase
