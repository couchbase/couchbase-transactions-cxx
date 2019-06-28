#pragma once

#include <string>

namespace couchbase
{
namespace transactions
{
    /**
     * The possible states for a transaction attempt.
     */
    enum AttemptState {
        /**
         * The attempt finished very early.
         */
        NOT_STARTED,

        /**
         * Any call to one of the mutation methods - <code>insert</code>, <code>replace</code>, <code>remove</code> - will update the
         * state to PENDING.
         */
        PENDING,

        /**
         * Set once the Active Transaction Record entry for this transaction has been updated to mark the transaction as
         * Aborted.
         */
        ABORTED,

        /**
         * Set once the Active Transaction Record entry for this transaction has been updated to mark the transaction as
         * Committed.
         */
        COMMITTED,

        /**
         * Set once the commit is fully completed.
         */
        COMPLETED,

        /**
         * Set once the commit is fully rolled back.
         */
        ROLLED_BACK
    };

    inline const char *attempt_state_name(AttemptState state)
    {
        switch (state) {
            case NOT_STARTED:
                return "NOT_STARTED";
            case PENDING:
                return "PENDING";
            case ABORTED:
                return "ABORTED";
            case COMMITTED:
                return "COMMITTED";
            case COMPLETED:
                return "COMPLETED";
            case ROLLED_BACK:
                return "ROLLED_BACK";
            default:
                throw std::string("unknown attempt state: ") + std::to_string(state);
        }
    }
} // namespace transactions
} // namespace couchbase
