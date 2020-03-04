#pragma once

namespace couchbase
{
namespace transactions
{
    /**
     * Gives additional information regarding a returned document's status.  The application is free to ignore any of them,
     * but may wish to take action on <code>AMBIGUOUS</code>.
     */
    enum class transaction_document_status {

        /**
         * The fetched document was not involved in a transaction.
         */
        NORMAL,

        /**
         * On fetch, the document was found to have staged data from a now-committed transaction, so the staged data has
         * been returned.
         */
        IN_TXN_COMMITTED,

        /**
         * On fetch, the document was found to have staged data from a non-committed transaction, so the document's content
         * has been returned rather than the staged content.
         */
        IN_TXN_OTHER,

        /**
         * The document has staged data from this transaction.  To support 'read your own writes', the staged data is
         * returned.
         */
        OWN_WRITE,

        /**
         * On fetch, the document was found to have staged data from a transaction.  However, that transaction's status
         * could not be determined in a timely fashion.  The application can decide whether to a) throw an exception to
         * cause the transaction logic to be retried (which is the safest option, and maximises consistency), b) use the
         * possibly stale data, which may be in the process of being
         * overwritten by another transaction (which improves availability).
         */
        AMBIGUOUS
    };

    inline const char* transaction_document_status_name(transaction_document_status status)
    {
        switch (status) {
            case transaction_document_status::NORMAL:
                return "NORMAL";
            case transaction_document_status::IN_TXN_COMMITTED:
                return "IN_TXN_COMMITTED";
            case transaction_document_status::IN_TXN_OTHER:
                return "IN_TXN_OTHER";
            case transaction_document_status::OWN_WRITE:
                return "OWN_WRITE";
            case transaction_document_status::AMBIGUOUS:
                return "AMBIGUOUS";
        }
    }
} // namespace transactions
} // namespace couchbase
