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

#include <chrono>
#include <list>
#include <memory>
#include <mutex>
#include <string>

#include <boost/optional.hpp>
#include <couchbase/client/bucket.hxx>
#include <couchbase/support.hxx>

namespace couchbase
{
class bucket;
// forward declare the internal pool class
template<typename T>
class pool;

// and the internal counter struct
struct instance_pool_event_counter;

static const size_t DEFAULT_CLUSTER_MAX_INSTANCES = 4;
static const size_t DEFAULT_BUCKET_MAX_INSTANCES = 4;

/**
 *  @brief Options for cluster connections
 *
 */
class cluster_options
{
  private:
    size_t max_instances_;
    size_t max_bucket_instances_;
    boost::optional<std::chrono::microseconds> kv_timeout_;
    instance_pool_event_counter* event_counter_;

  public:
    /**
     * @brief Default constructor for cluster_options
     */
    cluster_options()
      : max_instances_(DEFAULT_CLUSTER_MAX_INSTANCES)
      , max_bucket_instances_(DEFAULT_BUCKET_MAX_INSTANCES)
      , event_counter_(nullptr)
    {
    }

    /**
     *  @brief get maximum number of libcouchbase instances for this cluster.
     *
     *  The instances are created lazily - only when there is thread contention
     *  over them will more be created, up to this max.  When none are available,
     *  operations will block until one becomes available.
     *
     *  @return maximum number of libcouchbase instances to create for this cluster
     */
    CB_NODISCARD size_t max_instances() const
    {
        return max_instances_;
    }
    /**
     *  @brief Set maximum number of libcouchbase instances for this cluster.
     *
     *  @return reference to this options object, so the calls can be chained.
     */
    cluster_options& max_instances(size_t max)
    {
        max_instances_ = max;
        return *this;
    }
    /**
     *  @brief get maximum number of libcouchbase instances for this cluster.
     *
     *  The instances are created lazily - only when there is thread contention
     *  over them will more be created, up to this max.  When none are available,
     *  operations will block until one becomes available.
     *
     *  @return maximum number of libcouchbase instances to create for any bucket
     *          created from this cluster.
     */
    CB_NODISCARD size_t max_bucket_instances() const
    {
        return max_bucket_instances_;
    }
    /**
     *  @brief Set maximum number of libcouchbase instances for all buckets created
     *  from this cluster.
     *
     *  @return reference to this options object, so the calls can be chained.
     */
    cluster_options& max_bucket_instances(size_t max)
    {
        max_bucket_instances_ = max;
        return *this;
    }
    /**
     * @brief Default kv timeout
     *
     * This is the default kv timeout to use for any kv operation within the cluster
     * if it has not been specified in the options for that operation.
     *
     * @return The duration this cluster is using, if it was set.
     */
    CB_NODISCARD boost::optional<std::chrono::microseconds> kv_timeout() const
    {
        return kv_timeout_;
    }
    /**
     * @brief Set default kv timeout
     *
     * Sets the kv timeout to use for any kv operation within the cluster if it has not
     * been specified in the options for that operation.  The @ref result will indicate
     * that the operation timed out, @see @result::is_timeout().
     *
     * @param duration The desired duration to use.
     */
    template<typename T>
    cluster_options& kv_timeout(T duration)
    {
        kv_timeout_ = std::chrono::duration_cast<std::chrono::microseconds>(duration);
        return *this;
    }

    /**
     * @internal
     */
    CB_NODISCARD instance_pool_event_counter* event_counter() const
    {
        return event_counter_;
    }
    /**
     * @internal
     */
    cluster_options& event_counter(instance_pool_event_counter* counter)
    {
        event_counter_ = counter;
        return *this;
    }
};

class cluster
{
  private:
    lcb_st* lcb_;
    std::string cluster_address_;
    std::string user_name_;
    std::string password_;
    std::mutex mutex_;
    size_t max_bucket_instances_;
    std::list<std::shared_ptr<class bucket>> open_buckets_;
    std::unique_ptr<pool<lcb_st*>> instance_pool_;
    instance_pool_event_counter* event_counter_;
    boost::optional<std::chrono::microseconds> kv_timeout_;

  public:
    /**
     * @brief Create a new cluster and connect to it
     *
     * For now we are only connecting with a username and password, but this will be expanded
     * to include ssl, with or without cert verification, soon
     *
     * @param cluster_address Address of the cluster, say couchbase://1.2.3.4
     * @param user_name User name to use for this connection.
     * @param password Password for this user.
     */
    explicit cluster(std::string cluster_address,
                     std::string user_name,
                     std::string password,
                     const cluster_options& opts = cluster_options());
    /**
     *
     * @brief Copy cluster
     *
     * Creates a copy of the cluster, and connects.
     *
     * @param cluster The cluster to copy.
     */
    cluster(const cluster& cluster);

    /**
     * @brief Destroy a cluster
     *
     * Calls shutdown if necessary, and deletes the underlying connection to the cluster.
     *
     */
    ~cluster();
    /**
     *  @brief List buckets in this cluster.
     *
     * Returns a list of the names of all the buckets in this cluster.
     *
     * @return list of strings containing all the buckets in this cluster.
     */
    CB_NODISCARD std::list<std::string> buckets();
    /**
     *  @brief Open a connection to a bucket.
     *
     * This returns a shared pointer to a bucket.
     *
     * @param name Name of the bucket to connect to.
     * @return shared pointer to the bucket.
     */
    CB_NODISCARD std::shared_ptr<class bucket> bucket(const std::string& name);
    /**
     * @brief return the cluster address
     *
     * Helpful for debugging/logging.  Perhaps this will go away in favor of an operator<<
     * which is a bit more useful.
     *
     * @return A constant string containing the cluster address used for this cluster.
     */
    CB_NODISCARD const std::string cluster_address() const
    {
        return cluster_address_;
    }

    /**
     * @brief return maximum number of libcouchbase instances this cluster can use
     *
     * The cluster maintains a pool of instances, lazily created, which it uses to
     * communicate with the server.  Each instance will maintain a number of socket
     * connections.   Any cluster calls that need an instance will use one, making
     * it unavailable until the call is done with it.  See @ref cluster_options to
     * set this value.
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
     * @brief Default kv timeout
     *
     * @return The default kv timeout.
     */
    CB_NODISCARD std::chrono::microseconds default_kv_timeout() const;

    /**
     * @brief compare two clusters for equality
     */
    CB_NODISCARD bool operator==(const couchbase::cluster& other) const;
};
} // namespace couchbase
