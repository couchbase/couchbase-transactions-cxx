/*
 *     Copyright 2020 Couchbase, Inc.
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
#pragma once

#include <couchbase/client/cluster.hxx>
#include <couchbase/transactions/transaction_config.hxx>

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
        client_record(cluster& cluster, const transaction_config& config)
          : cluster_(cluster)
          , config_(config)
        {
        }

      private:
        cluster& cluster_;
        const transaction_config& config_;
    };
} // namespace transactions
} // namespace couchbase
