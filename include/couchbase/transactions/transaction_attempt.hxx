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

#include <couchbase/transactions/attempt_state.hxx>
#include <string>
#include <vector>

namespace couchbase
{
namespace transactions
{
    struct mutation_token {
        // Currently empty - tests only check for its existence.  That makes you wonder
        // why it is here at all, even just for testing, but there you go.   TODO: review
        // why/if it is actually needed.
    };

    struct transaction_attempt {
        std::string id;
        attempt_state state;
        std::vector<mutation_token> mutation_tokens;
        transaction_attempt();
        void add_mutation_token();
    };
} // namespace transactions
} // namespace couchbase
