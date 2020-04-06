#pragma once

#include <stdexcept>

namespace couchbase
{
namespace transactions
{

    class transaction_failed : public std::runtime_error
    {
      public:
        explicit transaction_failed(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };

    class document_already_in_transaction : public std::runtime_error
    {
      public:
        explicit document_already_in_transaction(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };

    class attempt_expired : public std::runtime_error
    {
      public:
        explicit attempt_expired(const std::string& what)
          : std::runtime_error(what)
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
