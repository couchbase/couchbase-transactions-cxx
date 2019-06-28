#pragma once

#include <libcouchbase/transactions/attempt_context.hxx>

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
      public:
        virtual void run(AttemptContext &ctx) = 0;
    };
} // namespace transactions
} // namespace couchbase
