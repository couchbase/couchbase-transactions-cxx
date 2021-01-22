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

#include <boost/optional.hpp>
#include <couchbase/client/result.hxx>
#include <couchbase/transactions/transaction_result.hxx>

#include <stdexcept>

namespace couchbase
{
namespace transactions
{
    /** @internal
     */
    class transaction_context;

    class retry_operation : public std::runtime_error
    {
      public:
        retry_operation(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };
    class retry_operation_timeout : public std::runtime_error
    {
      public:
        retry_operation_timeout(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };
    //  only used in ambiguity resolution during atr_commit
    class retry_atr_commit : public std::runtime_error
    {
      public:
        retry_atr_commit(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };

    enum external_exception {
        UNKNOWN = 0,
        ACTIVE_TRANSACTION_RECORD_ENTRY_NOT_FOUND,
        ACTIVE_TRANSACTION_RECORD_FULL,
        ACTIVE_TRANSACTION_RECORD_NOT_FOUND,
        DOCUMENT_ALREADY_IN_TRANSACTION,
        DOCUMENT_EXISTS_EXCEPTION,
        DOCUMENT_NOT_FOUND_EXCEPTION,
        NOT_SET,
        FEATURE_NOT_AVAILABLE_EXCEPTION,
        TRANSACTION_ABORTED_EXTERNALLY,
        PREVIOUS_OPERATION_FAILED,
        FORWARD_COMPATIBILITY_FAILURE
    };

    /**
     * @brief Base class for all exceptions expected to be raised from a transaction.
     *
     * Subclasses of this are the only exceptions that are raised out of the transaction lambda.
     */
    class transaction_exception : public std::runtime_error
    {
      private:
        const transaction_result result_;
        external_exception cause_;

      public:
        /**
         * @brief Construct from underlying exception.
         *
         * @param cause Underlying cause for this exception.
         * @param context The internal state of the transaction at the time of the exception.
         */
        explicit transaction_exception(const std::runtime_error& cause, const transaction_context& context);

        /**
         * @brief Internal state of transaction at time of exception
         *
         * @returns Internal state of transaction.
         */
        const transaction_result& get_transaction_result() const
        {
            return result_;
        }

        /**
         * @brief The cause of the exception
         *
         * @returns The underlying cause for this exception.
         */
        external_exception cause() const
        {
            return cause_;
        }
    };

    /**
     * @brief Transaction failed
     *
     * This is raised when the transaction doesn't time out, but fails for some other reason.
     */
    class transaction_failed : public transaction_exception
    {
      public:
        explicit transaction_failed(const std::runtime_error& cause, const transaction_context& context)
          : transaction_exception(cause, context)
        {
        }
    };

    /**
     * @brief Transaction expired.
     *
     * A transaction can expire if, for instance, a document in the transaction is also being mutated
     * in other transactions (or outside transactions).  The transaction will rollback and retry in this
     * situation, however if the conflicts persist it can expire before being successful.
     */
    class transaction_expired : public transaction_exception
    {
      public:
        explicit transaction_expired(const std::runtime_error& cause, const transaction_context& context)
          : transaction_exception(cause, context)
        {
        }
    };

    /**
     * @brief Transaction commit ambiguous.
     *
     * A transaction can, rarely, run into an error during the commit phase that is ambiguous.  For instance, a
     * write operation may timeout where we cannot be sure if the server performed the write or not.  If this happens
     * at other points in the transaction, we can retry but at the end of the commit phase, we raise this.
     */
    class transaction_commit_ambiguous : public transaction_exception
    {
      public:
        explicit transaction_commit_ambiguous(const std::runtime_error& cause, const transaction_context& context)
          : transaction_exception(cause, context)
        {
        }
    };

} // namespace transactions
} // namespace couchbase
