#pragma once

#include "transaction_context.hxx"
#include <couchbase/client/result.hxx>
#include <couchbase/transactions/exceptions.hxx>

namespace couchbase
{
namespace transactions
{
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

    class retry_operation_retries_exhausted : public std::runtime_error
    {
      public:
        retry_operation_retries_exhausted(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };
    enum error_class {
        FAIL_HARD = 0,
        FAIL_OTHER,
        FAIL_TRANSIENT,
        FAIL_AMBIGUOUS,
        FAIL_DOC_ALREADY_EXISTS,
        FAIL_DOC_NOT_FOUND,
        FAIL_PATH_NOT_FOUND,
        FAIL_CAS_MISMATCH,
        FAIL_WRITE_WRITE_CONFLICT,
        FAIL_ATR_FULL,
        FAIL_PATH_ALREADY_EXISTS,
        FAIL_EXPIRY
    };

    enum final_error { FAILED, EXPIRED, FAILED_POST_COMMIT, AMBIGUOUS };

    error_class error_class_from_result(const couchbase::result& res);

    external_exception external_exception_from_error_class(error_class ec);

    class client_error : public std::runtime_error
    {
      private:
        error_class ec_;
        uint32_t rc_;
        boost::optional<couchbase::result> res_;

      public:
        explicit client_error(const couchbase::result& res)
          : runtime_error(res.strerror())
          , ec_(error_class_from_result(res))
          , rc_(res.error())
          , res_(res)
        {
            assert(rc_ != 0 && "cannot throw client_error if there is no error");
        }
        explicit client_error(error_class ec, const std::string& what)
          : runtime_error(what)
          , ec_(ec)
          , rc_(0) // only slightly better than uninitialized.  Consider boost::optional<uint32_t>
        {
        }
        error_class ec() const
        {
            return ec_;
        }
        uint32_t rc() const
        {
            return rc_;
        }
        boost::optional<couchbase::result> res() const
        {
            return res_;
        }
    };

    // Prefer this as it reads better than throw client_error(FAIL_EXPIRY, ...)
    class attempt_expired : public client_error
    {
      public:
        attempt_expired(const std::string& what)
          : client_error(FAIL_EXPIRY, what)
        {
        }
    };

    /**
     * All exceptions within a transaction are, or are converted to, an exception
     * derived from this.  The transaciton logic then consumes them to decide to
     * retry, or rollback the transaction.
     */
    class transaction_operation_failed : public std::runtime_error
    {
      public:
        explicit transaction_operation_failed(error_class ec, const std::string& what)
          : std::runtime_error(what)
          , ec_(ec)
          , retry_(false)
          , rollback_(true)
          , to_raise_(FAILED)
          , cause_(external_exception_from_error_class(ec))
        {
        }
        explicit transaction_operation_failed(const client_error& client_err)
          : std::runtime_error(client_err.what())
          , ec_(client_err.ec())
          , retry_(false)
          , rollback_(true)
          , to_raise_(FAILED)
          , cause_(UNKNOWN)
        {
        }
        // Retry is false by default, this makes it true
        transaction_operation_failed& retry()
        {
            retry_ = true;
            validate();
            return *this;
        }

        // Rollback defaults to true, this sets it to false
        transaction_operation_failed& no_rollback()
        {
            rollback_ = false;
            validate();
            return *this;
        }

        // Defaults to FAILED, this sets it to EXPIRED
        transaction_operation_failed& expired()
        {
            to_raise_ = EXPIRED;
            validate();
            return *this;
        }

        // Defaults to FAILED, sets to FAILED_POST_COMMIT
        transaction_operation_failed& failed_post_commit()
        {
            to_raise_ = FAILED_POST_COMMIT;
            validate();
            return *this;
        }

        // Defaults to FAILED, sets AMBIGUOUS
        transaction_operation_failed& ambiguous()
        {
            to_raise_ = AMBIGUOUS;
            validate();
            return *this;
        }

        transaction_operation_failed& cause(external_exception cause)
        {
            cause_ = cause;
            validate();
            return *this;
        }

        error_class ec() const
        {
            return ec_;
        }

        bool should_rollback() const
        {
            return rollback_;
        }

        bool should_retry() const
        {
            return retry_;
        }

        external_exception cause() const
        {
            return cause_;
        }

        void do_throw(const transaction_context context) const
        {
            if (to_raise_ == FAILED_POST_COMMIT) {
                return;
            }
            switch (to_raise_) {
                case EXPIRED:
                    throw transaction_expired(*this, context);
                case AMBIGUOUS:
                    throw transaction_commit_ambiguous(*this, context);
                default:
                    throw transaction_failed(*this, context);
            }
        }

      private:
        error_class ec_;
        bool retry_;
        bool rollback_;
        final_error to_raise_;
        external_exception cause_;

        void validate()
        {
            // you can't retry without rollback.
            assert(!(retry_ && !rollback_));
        }
    };

    namespace internal
    {
        /**
         * Used only in testing: injects an error that will be handled as FAIL_HARD.
         *
         * This is not an error class the transaction library would ever raise
         * voluntarily.  It is designed to simulate an application crash or similar. The
         * transaction will not rollback and will stop abruptly.
         *
         * However, for testing purposes, a TransactionFailed will still be raised,
         * correct in all respects including the attempts field.
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
         * Used only in testing: injects an error that will be handled as
         * FAIL_AMBIGUOUS.
         *
         * E.g. either the server or SDK raised an error indicating the operation was
         * ambiguously successful.
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
         * Used only in testing: injects an error that will be handled as
         * FAIL_TRANSIENT.
         *
         * E.g. a transient server error that could be recovered with a retry of either
         * the operation or the transaction.
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
