#include <libcouchbase/transactions/client_record.hxx>

couchbase::transactions::client_record::client_record(couchbase::cluster &cluster, const couchbase::transactions::configuration &config)
    : cluster_(cluster), config_(config)
{
}
