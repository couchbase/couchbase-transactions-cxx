#pragma once

#include <libcouchbase/collection.hxx>

namespace couchbase
{
/**
 * Couchbase bucket.
 *
 * Exposes bucket-level operations and collections accessors.
 */
class Bucket
{
  public:
    Collection default_collection();
};
} // namespace couchbase
