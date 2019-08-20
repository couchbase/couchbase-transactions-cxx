#pragma once

#include <couchbase/client/cluster.hxx>
#include <couchbase/transactions/configuration.hxx>

namespace couchbase
{
namespace transactions
{
    /**
     * Represents the ClientRecord doc, a single document that contains an entry for every client (app) current participating in the cleanup
     * of 'lost' transactions.
     *
     * ClientRecord isn't as contended as it appears.  It's only read and written to by each client once per cleanup window (default for
     * this is 60 seconds).  It does remain a single point of failure, but with a sensible number of replicas this is unlikely to be a
     * problem.
     *
     * All writes are non-durable.  If a write is rolled back then it's not critical, it will just take a little longer to find lost txns.
     */
    class client_record
    {
      public:
        client_record(cluster &cluster, const configuration &config);

      private:
        cluster &cluster_;
        const configuration &config_;
    };
} // namespace transactions
} // namespace couchbase
