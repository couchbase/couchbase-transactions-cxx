#include <libcouchbase/transactions/transaction_context.hxx>
#include <libcouchbase/transactions/uid_generator.hxx>

couchbase::transactions::TransactionContext::TransactionContext()
{
    id_ = UidGenerator::next();
}

const std::string &couchbase::transactions::TransactionContext::id()
{
    return id_;
}
