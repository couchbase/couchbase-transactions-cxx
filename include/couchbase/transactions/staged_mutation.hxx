#pragma once

#include <mutex>
#include <string>
#include <vector>

#include <couchbase/client/mutate_in_spec.hxx>
#include <couchbase/transactions/transaction_document.hxx>

namespace couchbase
{
namespace transactions
{
    enum class staged_mutation_type { INSERT, REMOVE, REPLACE };

    class staged_mutation
    {
      private:
        transaction_document doc_;
        staged_mutation_type type_;
        nlohmann::json content_;

      public:
        template<typename Content>
        staged_mutation(transaction_document& doc, Content content, staged_mutation_type type)
          : doc_(std::move(doc))
          , content_(std::move(content))
          , type_(type)
        {
        }

        transaction_document& doc()
        {
            return doc_;
        }

        [[nodiscard]] const staged_mutation_type& type() const
        {
            return type_;
        }

        template<typename Content>
        const Content& content() const
        {
            return content_;
        }
    };

    class staged_mutation_queue
    {
      private:
        std::mutex mutex_;
        std::vector<staged_mutation> queue_;

      public:
        bool empty();
        void add(const staged_mutation& mutation);
        void extract_to(const std::string& prefix, std::vector<couchbase::mutate_in_spec>& specs);
        // TODO: deal with hooks - pass attempt_ctx here
        void commit();
        void iterate(std::function<void(staged_mutation&)>);

        staged_mutation* find_replace(std::shared_ptr<collection> collection, const std::string& id);
        staged_mutation* find_insert(std::shared_ptr<collection> collection, const std::string& id);
        staged_mutation* find_remove(std::shared_ptr<collection> collection, const std::string& id);
    };
} // namespace transactions
} // namespace couchbase
