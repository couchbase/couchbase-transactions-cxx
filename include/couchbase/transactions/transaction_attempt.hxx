#pragma once
#include <string>
#include <vector>
#include "attempt_state.hxx"
#include "uid_generator.hxx"

namespace couchbase {
    namespace transactions{
        struct mutation_token {
            // Currently empty - tests only check for its existence.  That makes you wonder
            // why it is here at all, even just for testing, but there you go.   TODO: review
            // why/if it is actually needed.
        };

        struct transaction_attempt {
            std::string id;
            attempt_state state;
            std::vector<mutation_token> mutation_tokens;
            transaction_attempt(): id(uid_generator::next()), state(attempt_state::NOT_STARTED) {};
            void add_mutation_token() {
                mutation_token t;
                mutation_tokens.push_back(t);
            }
        };
    } // namespace transactions
} // namespace couchbase

