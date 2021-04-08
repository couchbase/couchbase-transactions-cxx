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
#include "exceptions_internal.hxx"
#include "transaction_context.hxx"

#include <couchbase/transactions/exceptions.hxx>
#include <libcouchbase/couchbase.h>

namespace couchbase
{
namespace transactions
{
    external_exception external_exception_from_error_class(error_class ec)
    {
        switch (ec) {
            case FAIL_DOC_NOT_FOUND:
                return DOCUMENT_NOT_FOUND_EXCEPTION;
            case FAIL_DOC_ALREADY_EXISTS:
                return DOCUMENT_EXISTS_EXCEPTION;
            default:
                return UNKNOWN;
        }
    }

    error_class error_class_from_result(const couchbase::result& res)
    {
        uint32_t rc = res.error();
        assert(rc != LCB_SUCCESS);
        switch (rc) {
            case LCB_ERR_DOCUMENT_NOT_FOUND:
                return FAIL_DOC_NOT_FOUND;
            case LCB_ERR_DOCUMENT_EXISTS:
                return FAIL_DOC_ALREADY_EXISTS;
            case LCB_ERR_SUBDOC_PATH_NOT_FOUND:
                return FAIL_PATH_NOT_FOUND;
            case LCB_ERR_SUBDOC_PATH_EXISTS:
                return FAIL_PATH_ALREADY_EXISTS;
            case LCB_ERR_CAS_MISMATCH:
                return FAIL_CAS_MISMATCH;
            case LCB_ERR_VALUE_TOO_LARGE:
                return FAIL_ATR_FULL;
            case LCB_ERR_UNAMBIGUOUS_TIMEOUT:
            case LCB_ERR_NETWORK:
            case LCB_ERR_TIMEOUT:
            case LCB_ERR_TEMPORARY_FAILURE:
            case LCB_ERR_DURABLE_WRITE_IN_PROGRESS:
                return FAIL_TRANSIENT;
            case LCB_ERR_DURABILITY_AMBIGUOUS:
            case LCB_ERR_AMBIGUOUS_TIMEOUT:
            case LCB_ERR_REQUEST_CANCELED:
                return FAIL_AMBIGUOUS;
            default:
                return FAIL_OTHER;
        }
    }

    transaction_exception::transaction_exception(const std::runtime_error& cause, const transaction_context& context)
      : std::runtime_error(cause)
      , result_(context.get_transaction_result())
      , cause_(UNKNOWN)
    {
        auto txn_op = dynamic_cast<const transaction_operation_failed*>(&cause);
        if (nullptr != txn_op) {
            cause_ = txn_op->cause();
        }
    }
} // namespace transactions
} // namespace couchbase
