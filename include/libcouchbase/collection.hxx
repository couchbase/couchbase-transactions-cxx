#pragma once

#include <string>

namespace couchbase
{
class Collection
{
  private:
    std::string name_;

  public:
    Collection();

    void upsert(const std::string &id, const std::string &value);

    const std::string &name() const;
};
} // namespace couchbase
