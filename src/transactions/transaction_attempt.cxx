#include "transaction_attempt.hxx"
#include "uid_generator.hxx"

namespace couchbase
{
namespace transactions
{

    transaction_attempt::transaction_attempt()
      : id(uid_generator::next())
      , state(attempt_state::NOT_STARTED){};

} // namespace transactions
} // namespace couchbase
