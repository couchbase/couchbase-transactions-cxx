#pragma once

#include <string>
#include <mutex>
#include <vector>

#include <libcouchbase/transactions/transaction_document.hxx>
#include <libcouchbase/mutate_in_spec.hxx>

namespace couchbase
{
namespace transactions
{
    enum StagedMutationType { INSERT, REMOVE, REPLACE };

    class StagedMutation
    {
      private:
        TransactionDocument doc_;
        StagedMutationType type_;
        std::string content_;

      public:
        StagedMutation(TransactionDocument &doc, const std::string &content, StagedMutationType type);

        const TransactionDocument &doc() const;
        const StagedMutationType &type() const;
        const std::string &content() const;
    };

    class StagedMutationQueue
    {
      private:
        std::mutex mutex_;
        std::vector< StagedMutation > queue_;

      public:
        bool empty();
        void add(const StagedMutation &mutation);
        void extract_to(const std::string &prefix, std::vector< couchbase::MutateInSpec > &specs);

        StagedMutation *find_replace(Collection *collection, const std::string &id);
        StagedMutation *find_insert(Collection *collection, const std::string &id);
        StagedMutation *find_remove(Collection *collection, const std::string &id);
    };
} // namespace transactions
} // namespace couchbase
