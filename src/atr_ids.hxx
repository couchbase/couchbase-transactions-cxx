#pragma once

namespace couchbase
{
namespace transactions
{
    class AtrIds
    {
      public:
        static const std::string &atr_id_for_vbucket(int vbucket_id);
        static int vbucket_for_key(const std::string &key);
    };

} // namespace transactions
} // namespace couchbase
