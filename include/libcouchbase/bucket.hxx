#pragma once

#include <libcouchbase/collection.hxx>

struct lcb_st;

namespace couchbase
{
/**
 * Couchbase bucket.
 *
 * Exposes bucket-level operations and collections accessors.
 */
class bucket
{
    friend class collection;

  private:
    lcb_st *lcb_;

  public:
    bucket(lcb_st *instance, const std::string &name);
    collection *default_collection();
};
} // namespace couchbase
