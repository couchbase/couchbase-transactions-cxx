#include <couchbase/transactions/transaction_links.hxx>

std::ostream&
couchbase::transactions::operator<<(std::ostream& os, const transaction_links& links)
{
    os << "transaction_links{atr: " << links.atr_id_.value_or("none") << ", atr_bkt: " << links.atr_bucket_name_.value_or("none")
       << ", atr_coll: " << links.atr_collection_name_.value_or("none") << ", txn_id: " << links.staged_transaction_id_.value_or("none")
       << ", attempt_id: " << links.staged_attempt_id_.value_or("none") << ", crc32_of_staging:" << links.crc32_of_staging_.value_or("none")
       << "}";
    return os;
}
