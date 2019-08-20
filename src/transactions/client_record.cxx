#include <couchbase/transactions/client_record.hxx>

namespace tx = couchbase::transactions;

tx::client_record::client_record(couchbase::cluster &cluster, const tx::configuration &config) : cluster_(cluster), config_(config)
{
}
