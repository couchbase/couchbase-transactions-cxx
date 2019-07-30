#pragma once

#include <libcouchbase/collection.hxx>

struct lcb_st;

namespace couchbase
{

class cluster;

/**
 * Couchbase bucket.
 *
 * Exposes bucket-level operations and collections accessors.
 */
class bucket
{
    friend class cluster;
    friend class collection;

  private:
    lcb_st *lcb_;

    explicit bucket(lcb_st *instance, const std::string &name);

  public:
    collection *default_collection();
};
} // namespace couchbase
