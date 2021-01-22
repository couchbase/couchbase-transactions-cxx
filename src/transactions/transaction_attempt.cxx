#include "uid_generator.hxx"
#include <couchbase/transactions/transaction_attempt.hxx>

namespace couchbase
{
namespace transactions
{

    transaction_attempt::transaction_attempt()
      : id(uid_generator::next())
      , state(attempt_state::NOT_STARTED){};

    void transaction_attempt::add_mutation_token()
    {
        mutation_token t;
        mutation_tokens.push_back(t);
    }

} // namespace transactions
} // namespace couchbase
