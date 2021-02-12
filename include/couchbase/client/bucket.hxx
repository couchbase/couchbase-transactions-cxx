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

#include <couchbase/support.hxx>

#include <chrono>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace couchbase
{
class collection;
class cluster;

template<typename T>
class pool;

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
    std::unique_ptr<pool<lcb_st*>> instance_pool_;
    const std::string name_;
    std::vector<std::shared_ptr<class collection> > collections_;
    bucket(std::unique_ptr<pool<lcb_st*>>& instance_pool, const std::string& name, std::chrono::microseconds kv_timeout);
    std::shared_ptr<class collection> find_or_create_collection(const std::string& name);
    std::mutex mutex_;
    std::chrono::microseconds kv_timeout_;

  public:
    /**
     * Get the default collection for this bucket
     *
     * @return Returns a shared pointer to the default collection for this bucket.
     */
    CB_NODISCARD std::shared_ptr<class collection> default_collection();
    /**
     * @brief Get a collection by name.
     *
     * @param name of an existing collection in this bucket.
     * @return shared pointer to a the collection.
     */
    CB_NODISCARD std::shared_ptr<class collection> collection(const std::string& name);
    /**
     *  @brief Get collection name
     *
     *  Return the collection name.
     *
     * @return constant string containing this collection's name.  Note the default
     *         collection is _default.
     */
    CB_NODISCARD const std::string& name() const
    {
        return name_;
    };
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

    /**
     * @brief return maximum number of libcouchbase instances this bucket can use
     *
     * The bucket maintains a pool of instances, lazily created, which it uses to
     * communicate with the server.  Each instance will maintain a number of socket
     * connections.   Any cluster calls that need an instance will use one, making
     * it unavailable until the call is done with it.  See @ref cluster_options to
     * set this value, which the cluster uses when it creates the bucket.
     *
     * @return maximum number of libcouchbase instances the cluster can use.
     */
    CB_NODISCARD size_t max_instances() const;

    /**
     * @brief return current number of libcouchbase instances the cluster has created.
     *
     * @return total number of instances the cluster is maintaining.
     */
    CB_NODISCARD size_t instances() const;

    /**
     * @brief return the current number of libcouchbase instances that are not being used.
     *
     * @return current available instances.
     */
    CB_NODISCARD size_t available_instances() const;

    /**
     * @brief return default kv timeout
     *
     * @return The default kv timeout.  @see cluster_options::kv_timeout for setting this,
     * and @see common_options::timeout to use a different timeout on an operation.
     */
    CB_NODISCARD std::chrono::microseconds default_kv_timeout() const;

    /**
     * @brief convienence method to allow outputtng information about the bucket to an ostream or similar.
     */
    template<typename OStream>
    friend OStream& operator<<(OStream& os, const bucket& b)
    {
        os << "bucket:{";
        os << "name: " << b.name() << ",";
        os << "instance_pool: " << *b.instance_pool_;
        os << "}";
        return os;
    }

    CB_NODISCARD bool operator==(const bucket& b) const
    {
        return &b == this;
    }
};
}// namespace couchbase
