#pragma once

#include <libcouchbase/transactions/attempt_context.hxx>
#include <libcouchbase/transactions/result.hxx>

namespace couchbase
{
namespace transactions
{
    /**
     * Base interface of the transaction logic.
     *
     * The application have to override #run and use AttemptContext for all operations, that have to be included into transaction.
     */
    class Logic
    {
        virtual Result run(AttemptContext &ctx) = 0;
    };
} // namespace transactions
} // namespace couchbase
