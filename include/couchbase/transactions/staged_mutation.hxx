#pragma once

#include <string>
#include <mutex>
#include <vector>

#include <couchbase/transactions/transaction_document.hxx>
#include <couchbase/client/mutate_in_spec.hxx>

namespace couchbase
{
namespace transactions
{
    enum staged_mutation_type { INSERT, REMOVE, REPLACE };

    class staged_mutation
    {
      private:
        transaction_document doc_;
        staged_mutation_type type_;
        folly::dynamic content_;

      public:
        staged_mutation(transaction_document &doc, folly::dynamic content, staged_mutation_type type);

        transaction_document &doc();
        const staged_mutation_type &type() const;
        const folly::dynamic &content() const;
    };

    class staged_mutation_queue
    {
      private:
        std::mutex mutex_;
        std::vector<staged_mutation> queue_;

      public:
        bool empty();
        void add(const staged_mutation &mutation);
        void extract_to(const std::string &prefix, std::vector<couchbase::mutate_in_spec> &specs);
        void commit();

        staged_mutation *find_replace(collection *collection, const std::string &id);
        staged_mutation *find_insert(collection *collection, const std::string &id);
        staged_mutation *find_remove(collection *collection, const std::string &id);
    };
} // namespace transactions
} // namespace couchbase
