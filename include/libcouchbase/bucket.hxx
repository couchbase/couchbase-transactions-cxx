#pragma once

#include <libcouchbase/collection.hxx>

struct lcb_st;

namespace couchbase
{

class Cluster;

/**
 * Couchbase bucket.
 *
 * Exposes bucket-level operations and collections accessors.
 */
class Bucket
{
    friend class Cluster;
    friend class Collection;

  private:
    lcb_st *lcb_;

    explicit Bucket(lcb_st *instance, const std::string &name);

  public:
    Collection *default_collection();
};
} // namespace couchbase
