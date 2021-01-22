#pragma once

#include <couchbase/transactions/transaction_attempt.hxx>
#include <vector>

namespace couchbase
{
namespace transactions
{
    struct transaction_result {
        std::string transaction_id;
        std::string atr_id;
        std::string atr_collection;
        std::vector<transaction_attempt> attempts;
        bool unstaging_complete;
    };
} // namespace transactions
} // namespace couchbase
