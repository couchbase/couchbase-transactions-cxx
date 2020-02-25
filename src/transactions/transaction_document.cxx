#include <couchbase/transactions/transaction_document.hxx>

std::ostream&
couchbase::transactions::operator<<(std::ostream& os, const transaction_document& document)
{
    os << "transaction_docyment{id: " << document.id_ << ", cas: " << document.cas_ << ", status: " << document.status_
       << ", bucket: " << document.collection_.bucket_name() << ", coll: " << document.collection_.name() << ", links_: " << document.links_
       << "}";
    return os;
}
