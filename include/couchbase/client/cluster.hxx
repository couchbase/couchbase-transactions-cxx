#pragma once

#include <list>
#include <memory>
#include <mutex>
#include <string>

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

    void connect();

  public:
    explicit cluster(std::string cluster_address, std::string user_name, std::string password);
    ~cluster();

    std::list<std::string> buckets();
    std::shared_ptr<bucket> open_bucket(const std::string& name);
    void shutdown();
};
} // namespace couchbase
