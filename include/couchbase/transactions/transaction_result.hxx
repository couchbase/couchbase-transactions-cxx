#pragma once

#include <string>

namespace couchbase
{
namespace transactions
{
    /**
     * @brief Results of a transaction
     * @volatile
     *
     * Contains internal information on a transaction,
     * returned by @ref transactions::run()
     */
    struct transaction_result {
        std::string transaction_id;
        std::string atr_id;
        std::string atr_collection;
        bool unstaging_complete;
    };
} // namespace transactions
} // namespace couchbase
