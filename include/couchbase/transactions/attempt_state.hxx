#pragma once

#include <string>

namespace couchbase
{
namespace transactions
{
    /**
     * The possible states for a transaction attempt.
     */
    enum class attempt_state {
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

    inline const char *attempt_state_name(attempt_state state)
    {
        switch (state) {
            case attempt_state::NOT_STARTED:
                return "NOT_STARTED";
            case attempt_state::PENDING:
                return "PENDING";
            case attempt_state::ABORTED:
                return "ABORTED";
            case attempt_state::COMMITTED:
                return "COMMITTED";
            case attempt_state::COMPLETED:
                return "COMPLETED";
            case attempt_state::ROLLED_BACK:
                return "ROLLED_BACK";
            default:
                throw std::runtime_error("unknown attempt state");
        }
    }

    inline attempt_state attempt_state_value(const std::string& str)
    {
        if (str == "NOT_STARTED") {
            return attempt_state::NOT_STARTED;
        } else if (str == "PENDING") {
            return attempt_state::PENDING;
        } else if (str == "ABORTED") {
            return attempt_state::ABORTED;
        } else if (str == "COMMITTED") {
            return attempt_state::COMMITTED;
        } else if (str == "COMPLETED") {
            return attempt_state::COMPLETED;
        } else if (str == "ROLLED_BACK") {
            return attempt_state::ROLLED_BACK;
        } else {
            throw std::runtime_error("unknown attempt state: " + str);
        }
    }
} // namespace transactions
} // namespace couchbase
