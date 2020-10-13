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

#include <list>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include <couchbase/client/bucket.hxx>

struct lcb_st;

namespace couchbase
{
class bucket;

class cluster
{
  private:
    lcb_st* lcb_;
    std::string cluster_address_;
    std::string user_name_;
    std::string password_;
    std::mutex mutex_;
    std::vector<std::shared_ptr<class bucket>> open_buckets_;

    void connect();

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
    explicit cluster(std::string cluster_address, std::string user_name, std::string password);
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
    std::list<std::string> buckets();
    /**
     *  @brief Open a connection to a bucket.
     *
     * This returns a shared pointer to a bucket.
     *
     * @param name Name of the bucket to connect to.
     * @return shared pointer to the bucket.
     */
    std::shared_ptr<class bucket> bucket(const std::string& name);
    /**
     * @brief return the cluster address
     *
     * Helpful for debugging/logging.  Perhaps this will go away in favor of an operator<<
     * which is a bit more useful.
     *
     * @return A constant string containing the cluster address used for this cluster.
     */
    const std::string cluster_address() const
    {
        return cluster_address_;
    }

    /**
     * @brief Shutdown cluster
     *
     * Called in destructor, but exposed so one can call it whenever they want to free up the resources.
     * Note there is no reconnect logic in place yet, so this object cannot be used after this.  Look
     * for changes to this behavior later.
     */
    void shutdown();

    bool operator==(const couchbase::cluster&  other) const;
};
} // namespace couchbase
