#pragma once

#include <cstdint>
#include <string>

#include <nlohmann/json.hpp>

#include <couchbase/transactions/transaction_fields.hxx>

namespace couchbase
{
namespace transactions
{
    struct doc_record {
      public:
        doc_record(std::string bucket_name, std::string scope_name, std::string collection_name, std::string id)
          : bucket_name_(std::move(bucket_name))
          , scope_name_(std::move(scope_name))
          , collection_name_(std::move(collection_name))
          , id_(std::move(id))
        {
        }

        [[nodiscard]] const std::string& bucket_name() const
        {
            return bucket_name_;
        }

        [[nodiscard]] const std::string& id() const
        {
            return id_;
        }

        static doc_record create_from(nlohmann::json& obj)
        {
            std::string bucket_name = obj[ATR_FIELD_PER_DOC_BUCKET].get<std::string>();
            std::string scope_name = obj[ATR_FIELD_PER_DOC_SCOPE].get<std::string>();
            std::string collection_name = obj[ATR_FIELD_PER_DOC_COLLECTION].get<std::string>();
            std::string id = obj[ATR_FIELD_PER_DOC_ID].get<std::string>();
            return doc_record(bucket_name, scope_name, collection_name, id);
        }

      private:
        std::string bucket_name_;
        std::string scope_name_;
        std::string collection_name_;
        std::string id_;
    };
} // namespace transactions
} // namespace couchbase
