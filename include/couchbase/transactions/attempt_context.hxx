#pragma once

#include <string>
#include <vector>
#include <mutex>

#include <couchbase/client/collection.hxx>
#include <couchbase/transactions/transaction_document.hxx>
#include <couchbase/transactions/transaction_context.hxx>
#include <couchbase/transactions/attempt_state.hxx>
#include <couchbase/transactions/staged_mutation.hxx>
#include <couchbase/transactions/configuration.hxx>

namespace couchbase
{
namespace transactions
{
    /**
     * Provides methods to allow an application's transaction logic to read, mutate, insert and delete documents, as well as commit or
     * rollback the transaction.
     */
    class attempt_context
    {
      private:
        transaction_context &txctx_;
        const configuration &config_;
        std::optional<std::string> atr_id_;
        collection *atr_collection_;
        bool is_done_;
        attempt_state state_;
        std::string id_;
        staged_mutation_queue staged_mutations_;

        void init_atr_if_needed(collection *collection, const std::string &id);

      public:
        attempt_context(transaction_context &transaction_ctx, const configuration &config);

        /**
         * Gets a document from the specified Couchbase collection matching the specified id.
         *
         * @param collection the Couchbase collection the document exists on
         * @param id the document's ID
         * @return an TransactionDocument containing the document
         */
        std::optional<transaction_document> get(collection *collection, const std::string &id);

        /**
         * Mutates the specified document with new content, using the document's last TransactionDocument#cas().
         *
         * The mutation is staged until the transaction is committed.  That is, any read of the document by any Couchbase component will see
         * the document's current value, rather than this staged or 'dirty' data.  If the attempt is rolled back, the staged mutation will
         * be removed.
         *
         * This staged data effectively locks the document from other transactional writes until the attempt completes (commits or rolls
         * back).
         *
         * If the mutation fails, the transaction will automatically rollback this attempt, then retry.
         *
         * @param document the doc to be updated
         * @param content the content to replace the doc with.
         * @return the document, updated with its new CAS value.
         */
        transaction_document replace(collection *collection, const transaction_document &document, const folly::dynamic &content);

        /**
         * Inserts a new document into the specified Couchbase collection.
         *
         * As with #replace, the insert is staged until the transaction is committed.  Due to technical limitations it is not as possible to
         * completely hide the staged data from the rest of the Couchbase platform, as an empty document must be created.
         *
         * This staged data effectively locks the document from other transactional writes until the attempt completes
         * (commits or rolls back).
         *
         * @param collection the Couchbase collection in which to insert the doc
         * @param id the document's unique ID
         * @param content the content to insert
         * @return the doc, updated with its new CAS value and ID, and converted to a TransactionDocument
         */
        transaction_document insert(collection *collection, const std::string &id, const folly::dynamic &content);

        /**
         * Removes the specified document, using the document's last TransactionDocument#cas
         *
         * As with {@link #replace}, the remove is staged until the transaction is committed.  That is, the document will continue to exist,
         * and the rest of the Couchbase platform will continue to see it.
         *
         * This staged data effectively locks the document from other transactional writes until the attempt completes (commits or rolls
         * back).
         *
         * @param document the document to be removed
         */
        void remove(collection *collection, transaction_document &document);

        /**
         * Commits the transaction.  All staged replaces, inserts and removals will be written.
         *
         * After this, no further operations are permitted on this instance, and they will result in an
         * exception that will, if not caught in the transaction logic, cause the transaction to
         * fail.
         */
        void commit();
        bool is_done();

        const std::string &id();
    };
} // namespace transactions
} // namespace couchbase
