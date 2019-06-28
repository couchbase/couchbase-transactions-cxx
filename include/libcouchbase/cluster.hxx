#pragma once

#include <string>
#include <libcouchbase/bucket.hxx>

struct lcb_st;

namespace couchbase
{
class Bucket;

class Cluster
{
  private:
    lcb_st *lcb_;
    std::string cluster_address_;
    std::string user_name_;
    std::string password_;

    void connect();

  public:
    explicit Cluster(const std::string &cluster_address, const std::string &user_name, const std::string &password);
    ~Cluster();

    Bucket* bucket(const std::string &name);
    void shutdown();
};
} // namespace couchbase
