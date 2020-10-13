/*
 *     Copyright 2020 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

#pragma once

#include <memory>
#include <string>
#include <vector>

struct lcb_st;

namespace couchbase
{
class collection;
class cluster;
/**
 * Couchbase bucket.
 *
 * Exposes bucket-level operations and collections accessors.
 */
class bucket : public std::enable_shared_from_this<bucket>
{
    friend class collection;
    friend class cluster;

  private:
    lcb_st* lcb_;
    const std::string name_;
    std::vector<std::shared_ptr<class collection> > collections_;
    bucket(lcb_st* instance, const std::string& name);
    std::shared_ptr<class collection> find_or_create_collection(const std::string& name);
  public:
    /**
     * Get the default collection for this bucket
     *
     * @return Returns a shared pointer to the default collection for this bucket.
     */
    std::shared_ptr<class collection> default_collection();
    /**
     * @brief Get a collection by name.
     *
     * @param name of an existing collection in this bucket.
     * @return shared pointer to a the collection.
     */
    std::shared_ptr<class collection> collection(const std::string& name);
    /**
     *  @brief Get collection name
     *
     *  Return the collection name.
     *
     * @return constant string containing this collection's name.  Note the default
     *         collection is _default.
     */
    const std::string name() const { return name_; };
    /**
     * @brief Close connection to this bucket
     *
     * Once you call this, this object will no longer be connected to the cluster.  This
     * means any collection objects created from this bucket (all of whom share this
     * connection) will also not be connected.  Called in destructor, but here in case you
     * one needs it
     */
    void close();
    /**
     *  @brief Destroy the bucket
     *
     * Calls close(), which disconnects the bucket from the cluster, then destroys the object.
     */
    ~bucket();
    bool operator==(const bucket& b) const { return lcb_ == b.lcb_; }
};
}// namespace couchbase
