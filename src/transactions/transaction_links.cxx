#include <couchbase/transactions/transaction_links.hxx>

std::ostream&
couchbase::transactions::operator<<(std::ostream& os, const transaction_links& links)
{
    os << "transaction_links{atr: " << links.atr_id_ << ", atr_bkt: " << links.atr_bucket_name_ << ", atr_scp: " << links.atr_scope_name_
       << ", atr_coll: " << links.atr_collection_name_ << ", ver: " << links.staged_version_ << "}";
    return os;
}
