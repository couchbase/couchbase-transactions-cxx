/*
 *     Copyright 2020 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include <string>

#include <couchbase/client/collection.hxx>
#include <couchbase/transactions/transaction_document.hxx>

namespace couchbase
{
namespace transactions
{
    /**
     *
     */
    class attempt_context
    {
      public:
        /**
         * Gets a document from the specified Couchbase collection matching the specified id.
         *
         * @param collection the Couchbase collection the document exists on
         * @param id the document's ID
         * @return an TransactionDocument containing the document
         */
        virtual transaction_document get(std::shared_ptr<collection> collection, const std::string& id) = 0;

        /**
         * Gets a document from the specified Couchbase collection matching the specified id.
         *
         * @param collection the Couchbase collection the document exists on
         * @param id the document's ID
         * @return a TransactionDocument containing the document, if it exists.
         */
        virtual boost::optional<transaction_document> get_optional(std::shared_ptr<collection> collection, const std::string& id) = 0;

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
        template<typename Content>
        transaction_document replace(std::shared_ptr<collection> collection, const transaction_document& document, const Content& content)
        {
            nlohmann::json json_content = content;
            return replace_raw(collection, document, json_content);
        }
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
        template<typename Content>
        transaction_document insert(std::shared_ptr<collection> collection, const std::string& id, const Content& content)
        {
            nlohmann::json json_content = content;
            return insert_raw(collection, id, json_content);
        }
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
        virtual void remove(std::shared_ptr<couchbase::collection> collection, transaction_document& document) = 0;

        /**
         * Commits the transaction.  All staged replaces, inserts and removals will be written.
         *
         * After this, no further operations are permitted on this instance, and they will result in an
         * exception that will, if not caught in the transaction logic, cause the transaction to
         * fail.
         */
        virtual void commit() = 0;

        /**
         * Rollback the transaction.  All staged mutations will be unstaged.
         *
         * Typically, this is called internally to rollback transaction when errors occur in the lambda.  Though
         * it can be called explicitly from the app logic within the transaction as well, perhaps that is better
         * modeled as a custom exception that you raise instead.
         */
        virtual void rollback() = 0;

      protected:
        virtual transaction_document insert_raw(std::shared_ptr<collection> collection,
                                                const std::string& id,
                                                const nlohmann::json& content) = 0;

        virtual transaction_document replace_raw(std::shared_ptr<collection> collection,
                                                 const transaction_document& document,
                                                 const nlohmann::json& content) = 0;
    };

} // namespace transactions
} // namespace couchbase
