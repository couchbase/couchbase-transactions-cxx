#pragma once

#include <string>
#include <libcouchbase/bucket.hxx>

struct lcb_st;

namespace couchbase
{
class bucket;

class cluster
{
  private:
    lcb_st *lcb_;
    std::string cluster_address_;
    std::string user_name_;
    std::string password_;

    void connect();

  public:
    explicit cluster(std::string cluster_address, std::string user_name, std::string password);
    ~cluster();

    bucket *open_bucket(const std::string &name);
    void shutdown();
};
} // namespace couchbase
