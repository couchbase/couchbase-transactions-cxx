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

#include <thread>

namespace couchbase
{
class cluster;

namespace transactions
{
    class transactions_cleanup
    {
      public:
        transactions_cleanup(couchbase::cluster& cluster, const transaction_config& config);
        ~transactions_cleanup();

        CB_NODISCARD couchbase::cluster& cluster()
        {
            return cluster_;
        };

      private:
        void lost_attempts_loop();

        couchbase::cluster& cluster_;
        const transaction_config& config_;

        std::thread lost_attempts_thr;
        const std::string client_uuid_;

        bool running{ false };
    };
} // namespace transactions
} // namespace couchbase
