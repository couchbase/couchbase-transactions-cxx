#include <libcouchbase/transactions/transaction_context.hxx>
#include <libcouchbase/transactions/uid_generator.hxx>

couchbase::transactions::transaction_context::transaction_context()
{
    id_ = uid_generator::next();
}

const std::string &couchbase::transactions::transaction_context::id()
{
    return id_;
}
