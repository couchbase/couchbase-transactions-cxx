#pragma once

#include <stdexcept>
#include <boost/optional.hpp>

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

    enum final_error {
        FAILED,
        EXPIRED
    };

    /**
     * External excepitons
     *
     * These are the only exceptions that are raised out of the transaction lambda
     */
    class transaction_failed : public std::runtime_error
    {
      public:
        explicit transaction_failed(const std::runtime_error& cause)
          : std::runtime_error(cause)
        {
        }
    };

    class transaction_expired: public std::runtime_error
    {
      public:
        explicit transaction_expired(const std::runtime_error& cause)
            : std::runtime_error(cause)
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
        // TODO: prevent both retry and rollback from being true.
        explicit error_wrapper(error_class ec, const std::string& what, bool retry=false, bool rollback=true, final_error to_raise=FAILED)
            : std::runtime_error(what), _ec(ec), _retry(retry), _rollback(rollback), _to_raise(to_raise) {}
        explicit error_wrapper(error_class ec, const std::runtime_error& cause, bool retry=false, bool rollback=true, final_error to_raise=FAILED)
            : std::runtime_error(cause), _ec(ec), _retry(retry), _rollback(rollback), _to_raise(to_raise) {}

        bool should_retry() {
            return _retry;
        }
        bool should_rollback() {
            return _rollback;
        }
        void do_throw() {
            throw _to_raise == FAILED ? throw transaction_failed(*this) : transaction_expired(*this);
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
          : error_wrapper(FAIL_WRITE_WRITE_CONFLICT, what, true, false)
        {
        }
    };

    class attempt_expired : public error_wrapper
    {
      public:
        explicit attempt_expired(const std::string& what)
          : error_wrapper(FAIL_EXPIRY, what)
        {
        }
    };

    class attempt_exception : public std::runtime_error
    {
      public:
        explicit attempt_exception(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };

    class client_error : public std::runtime_error
    {
      public:
        explicit client_error(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };

    class transaction_commit_ambiguous: public std::runtime_error
    {
      public:
        explicit transaction_commit_ambiguous(const std::string& what)
            : std::runtime_error(what)
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
        class test_fail_hard : public std::runtime_error
        {
          public:
            explicit test_fail_hard()
              : std::runtime_error("Injecting a FAIL_HARD error")
            {
            }
        };

        /**
         * Used only in testing: injects an error that will be handled as FAIL_AMBIGUOUS.
         *
         * E.g. either the server or SDK raised an error indicating the operation was ambiguously successful.
         */
        class test_fail_ambiguous : public std::runtime_error
        {
          public:
            explicit test_fail_ambiguous()
              : std::runtime_error("Injecting a FAIL_AMBIGUOUS error")
            {
            }
        };

        /**
         * Used only in testing: injects an error that will be handled as FAIL_FAST.
         *
         * E.g. an unrecoverable fault, such as a DocumentNotFoundException when trying to get a doc.
         */
        class test_fail_fast : public std::runtime_error
        {
          public:
            explicit test_fail_fast()
              : std::runtime_error("Injecting a FAIL_FAST error")
            {
            }
        };

        /**
         * Used only in testing: injects an error that will be handled as FAIL_RETRY.
         *
         * E.g. a transient server error that could be recovered with a retry of either the operation or the transaction.
         */
        class test_fail_retry : public std::runtime_error
        {
          public:
            explicit test_fail_retry()
              : std::runtime_error("Injecting a FAIL_RETRY error")
            {
            }
        };
    } // namespace internal

} // namespace transactions
} // namespace couchbase
