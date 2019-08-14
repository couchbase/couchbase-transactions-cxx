#pragma once

#include <string>

namespace couchbase
{
namespace transactions
{
    // Fields in the Active Transaction Records
    // These are keep as brief as possible, more important to reduce changes of doc overflowing
    // than to preserve human debuggability
    static const std::string ATR_FIELD_ATTEMPTS = "attempts";
    static const std::string ATR_FIELD_STATUS = "st";
    static const std::string ATR_FIELD_START_TIMESTAMP = "tst";
    static const std::string ATR_FIELD_EXPIRES_AFTER_MSECS = "exp";
    static const std::string ATR_FIELD_START_COMMIT = "tsc";
    static const std::string ATR_FIELD_TIMESTAMP_COMPLETE = "tsco";
    static const std::string ATR_FIELD_TIMESTAMP_ROLLBACK_START = "tsrs";
    static const std::string ATR_FIELD_TIMESTAMP_ROLLBACK_COMPLETE = "tsrc";
    static const std::string ATR_FIELD_DOCS_INSERTED = "ins";
    static const std::string ATR_FIELD_DOCS_REPLACED = "rep";
    static const std::string ATR_FIELD_DOCS_REMOVED = "rem";
    static const std::string ATR_FIELD_PER_DOC_ID = "id";
    static const std::string ATR_FIELD_PER_DOC_CAS = "cas";
    static const std::string ATR_FIELD_PER_DOC_BUCKET = "bkt";
    static const std::string ATR_FIELD_PER_DOC_SCOPE = "scp";
    static const std::string ATR_FIELD_PER_DOC_COLLECTION = "col";

    // Fields inside regular docs that are part of a transaction
    static const std::string TRANSACTION_INTERFACE_PREFIX_ONLY = "txn";
    static const std::string TRANSACTION_INTERFACE_PREFIX = TRANSACTION_INTERFACE_PREFIX_ONLY + ".";
    static const std::string TRANSACTION_RESTORE_PREFIX_ONLY = TRANSACTION_INTERFACE_PREFIX_ONLY + ".restore";
    static const std::string TRANSACTION_RESTORE_PREFIX = TRANSACTION_RESTORE_PREFIX_ONLY + ".";
    static const std::string TRANSACTION_ID = TRANSACTION_INTERFACE_PREFIX + "id.txn";
    static const std::string ATTEMPT_ID = TRANSACTION_INTERFACE_PREFIX + "id.atmpt";
    static const std::string STAGED_VERSION = TRANSACTION_INTERFACE_PREFIX + "ver";
    static const std::string ATR_ID = TRANSACTION_INTERFACE_PREFIX + "atr_id";
    static const std::string ATR_BUCKET_NAME = TRANSACTION_INTERFACE_PREFIX + "atr_bkt";

    // The current plan is:
    // 6.5 and below: write metadata docs to the default collection
    // 7.0 and above: write them to the system collection, and migrate them over
    // Adding scope and collection metadata fields to try and future proof
    static const std::string ATR_SCOPE_NAME = TRANSACTION_INTERFACE_PREFIX + "atr_scope";
    static const std::string ATR_COLL_NAME = TRANSACTION_INTERFACE_PREFIX + "atr_col";
    static const std::string STAGED_DATA = TRANSACTION_INTERFACE_PREFIX + "staged";
    static const std::string TYPE = TRANSACTION_INTERFACE_PREFIX + "op.type";
    static const std::string PRE_TXN_CAS = TRANSACTION_RESTORE_PREFIX + "CAS";
    static const std::string PRE_TXN_REVID = TRANSACTION_RESTORE_PREFIX + "revid";
    static const std::string PRE_TXN_EXPTIME = TRANSACTION_RESTORE_PREFIX + "exptime";

    static const std::string STAGED_DATA_REMOVED_VALUE = "<<REMOVED>>";
} // namespace transactions
} // namespace couchbase
