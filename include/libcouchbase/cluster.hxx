#pragma once

#include <string>

#include <libcouchbase/bucket.hxx>

namespace couchbase
{
class Cluster
{
  public:
    explicit Cluster(std::string cluster_address);

    void authenticate(std::string user_name, std::string password);
    Bucket bucket(std::string name);
    void shutdown();
};
} // namespace couchbase
