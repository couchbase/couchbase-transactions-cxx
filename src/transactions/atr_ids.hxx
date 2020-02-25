#pragma once

#include <vector>

namespace couchbase
{
namespace transactions
{
    class atr_ids
    {
      public:
        static const std::string& atr_id_for_vbucket(int vbucket_id);
        static int vbucket_for_key(const std::string& key);
        static const std::vector<std::string>& all();
    };

} // namespace transactions
} // namespace couchbase
