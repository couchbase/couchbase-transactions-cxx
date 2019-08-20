#include <couchbase/transactions/transaction_context.hxx>
#include <couchbase/transactions/uid_generator.hxx>

namespace tx = couchbase::transactions;

tx::transaction_context::transaction_context()
{
    id_ = uid_generator::next();
}

const std::string &tx::transaction_context::id()
{
    return id_;
}
