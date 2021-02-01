#pragma once

#include <string>

namespace couchbase
{
namespace transactions
{
    struct transaction_result {
        std::string transaction_id;
        std::string atr_id;
        std::string atr_collection;
        bool unstaging_complete;
    };
} // namespace transactions
} // namespace couchbase
