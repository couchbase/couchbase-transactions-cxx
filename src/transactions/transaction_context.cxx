#include <couchbase/transactions/transaction_context.hxx>
#include <couchbase/transactions/uid_generator.hxx>

namespace tx = couchbase::transactions;

tx::transaction_context::transaction_context() : id_(uid_generator::next())
{
}

const std::string &tx::transaction_context::id()
{
    return id_;
}

size_t tx::transaction_context::num_attempts() const
{
    return attempts_.size();
}

boost::log::sources::logger_mt &couchbase::transactions::transaction_context::logger()
{
    return logger_;
}
