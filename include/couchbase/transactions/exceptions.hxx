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
#include <couchbase/transactions/transaction_context.hxx>
#include <spdlog/spdlog.h>
#include <stdexcept>

namespace couchbase
{
namespace transactions
{
    enum error_class {
        FAIL_OTHER,
        FAIL_TRANSIENT,
        FAIL_DOC_NOT_FOUND,
        FAIL_DOC_ALREADY_EXISTS,
        FAIL_PATH_NOT_FOUND,
        FAIL_PATH_ALREADY_EXISTS,
        FAIL_WRITE_WRITE_CONFLICT,
        FAIL_CAS_MISMATCH,
        FAIL_HARD,
        FAIL_AMBIGUOUS,
        FAIL_EXPIRY,
        FAIL_ATR_FULL
    };

    error_class error_class_from_result(const couchbase::result& res);

    class client_error : public std::runtime_error
    {
      private:
          error_class _ec;
          uint32_t _rc;
      public:
        explicit client_error(const couchbase::result& res)
            : runtime_error(res.strerror())
            , _ec(error_class_from_result(res))
            , _rc(res.rc)
        {
        }
        explicit client_error(error_class ec, const std::string& what)
            : runtime_error(what)
            , _ec(ec)
        {

            spdlog::trace("creating client error with ec {}", _ec);
        }
        error_class ec() const
        {
            return _ec;
        }
        uint32_t rc() const
        {
            return _rc;
        }
    };

    enum final_error { FAILED, EXPIRED, FAILED_POST_COMMIT };

    /**
     * External excepitons
     *
     * These are the only exceptions that are raised out of the transaction lambda
     */
    class transaction_base : public std::runtime_error
    {
      private:
        const transaction_context _context;

      public:
        explicit transaction_base(const std::runtime_error& cause, const transaction_context& context)
          : std::runtime_error(cause)
          , _context(context)
        {
        }
        const transaction_context& get_transaction_context() const
        {
            return _context;
        }
    };

    class transaction_failed : public transaction_base
    {
      public:
        explicit transaction_failed(const std::runtime_error& cause, const transaction_context& context)
          : transaction_base(cause, context)
        {
        }
    };

    class transaction_expired : public transaction_base
    {
      public:
        explicit transaction_expired(const std::runtime_error& cause, const transaction_context& context)
          : transaction_base(cause, context)
        {
        }
    };

    /** error_wrapper
     *
     * All exceptions within a transaction are, or are converted to an exception derived
     * from this.  The transaciton logic then consumes them to decide to retry, or rollback
     * the transaction.
     */
    class error_wrapper : public std::runtime_error
    {
      public:
        explicit error_wrapper(error_class ec,
                               const std::string& what,
                               bool retry = false,
                               bool rollback = true,
                               final_error to_raise = FAILED)
            : std::runtime_error(what)
            , _ec(ec)
            , _retry(retry)
            , _rollback(rollback)
            , _to_raise(to_raise)
            {
                validate();
            }
        explicit error_wrapper(const client_error& client_err,
                               bool retry = false,
                               bool rollback = true,
                               final_error to_raise = FAILED)
            : std::runtime_error(client_err.what())
            , _ec(client_err.ec())
            , _retry(retry)
            , _rollback(rollback)
            , _to_raise(to_raise)
            {
                validate();
            }
        explicit error_wrapper(error_class ec,
                               const std::runtime_error& cause,
                               bool retry = false,
                               bool rollback = true,
                               final_error to_raise = FAILED)
            : std::runtime_error(cause)
            , _ec(ec)
            , _retry(retry)
            , _rollback(rollback)
            , _to_raise(to_raise)
            {
                validate();
            }

        void validate() {
            // you can't retry without rollback.
            assert(!(_retry && !_rollback));
        }

        bool should_retry() const
        {
            return _retry;
        }
        bool should_rollback() const
        {
            return _rollback;
        }
        void do_throw(const transaction_context context) const
        {
            if (_to_raise == FAILED_POST_COMMIT) {
                spdlog::trace("failed post commit - not throwing exception");
                return;
            }
            spdlog::trace("throwing final error {}", _to_raise == FAILED ? "FAILED" : "EXPIRED");
            throw _to_raise == FAILED ? throw transaction_failed(*this, context) : transaction_expired(*this, context);
        }
        error_class ec() const
        {
            return _ec;
        }

      private:
        error_class _ec;
        bool _retry;
        bool _rollback;
        final_error _to_raise;
    };

    class document_already_in_transaction : public error_wrapper
    {
      public:
        /**
         * This is retryable - note the error wrapper constructor has true for retry and false for rollback
         */
        explicit document_already_in_transaction(const std::string& what)
          : error_wrapper(FAIL_WRITE_WRITE_CONFLICT, what, true, true)
        {
        }
    };

    class attempt_expired : public error_wrapper
    {
      public:
        explicit attempt_expired(const std::string& what)
          : error_wrapper(FAIL_EXPIRY, what, false, true, EXPIRED)
        {
        }
    };

    namespace internal
    {
        /**
         * Used only in testing: injects an error that will be handled as FAIL_HARD.
         *
         * This is not an error class the transaction library would ever raise voluntarily.  It is designed to simulate an application crash
         * or similar.  The transaction will not rollback and will stop abruptly.
         *
         * However, for testing purposes, a TransactionFailed will still be raised, correct in all respects including the attempts field.
         */
        class test_fail_hard : public client_error
        {
          public:
            explicit test_fail_hard()
              : client_error(FAIL_HARD, "Injecting a FAIL_HARD error")
            {
            }
        };

        /**
         * Used only in testing: injects an error that will be handled as FAIL_AMBIGUOUS.
         *
         * E.g. either the server or SDK raised an error indicating the operation was ambiguously successful.
         */
        class test_fail_ambiguous : public client_error
        {
          public:
            explicit test_fail_ambiguous()
              : client_error(FAIL_AMBIGUOUS, "Injecting a FAIL_AMBIGUOUS error")
            {
            }
        };

        /**
         * Used only in testing: injects an error that will be handled as FAIL_TRANSIENT.
         *
         * E.g. a transient server error that could be recovered with a retry of either the operation or the transaction.
         */
        class test_fail_transient : public client_error
        {
          public:
            explicit test_fail_transient()
              : client_error(FAIL_TRANSIENT, "Injecting a FAIL_TRANSIENT error")
            {
            }
        };

        /**
         * Used only in testing: injects an error that will be handled as FAIL_OTHER.
         *
         * E.g. an error which is not retryable.
         */
        class test_fail_other : public client_error
        {
          public:
            explicit test_fail_other()
              : client_error(FAIL_OTHER, "Injecting a FAIL_OTHER error")
            {
            }
        };
    } // namespace internal

} // namespace transactions
} // namespace couchbase
