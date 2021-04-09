/*
 *     Copyright 2021 Couchbase, Inc.
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

#include <cstdint>
#include <string>

#include "transaction_fields.hxx"
#include <couchbase/internal/nlohmann/json.hpp>
#include <couchbase/support.hxx>

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

        CB_NODISCARD const std::string& bucket_name() const
        {
            return bucket_name_;
        }

        CB_NODISCARD const std::string& id() const
        {
            return id_;
        }

        CB_NODISCARD const std::string collection_name() const
        {
            return collection_name_;
        }

        static doc_record create_from(nlohmann::json& obj)
        {
            std::string bucket_name = obj[ATR_FIELD_PER_DOC_BUCKET].get<std::string>();
            std::string scope_name = obj[ATR_FIELD_PER_DOC_SCOPE].get<std::string>();
            std::string collection_name = obj[ATR_FIELD_PER_DOC_COLLECTION].get<std::string>();
            std::string id = obj[ATR_FIELD_PER_DOC_ID].get<std::string>();
            return doc_record(bucket_name, scope_name, collection_name, id);
        }

        template<typename OStream>
        friend OStream& operator<<(OStream& os, const doc_record& dr)
        {
            os << "doc_record{";
            os << "bucket_name:" << dr.bucket_name_ << ",";
            os << "collection_name:" << dr.collection_name_ << ",";
            os << "scope_name:" << dr.scope_name_ << ",";
            os << "id:" << dr.id_ << ",";
            os << "}";
            return os;
        }

      private:
        std::string bucket_name_;
        std::string scope_name_;
        std::string collection_name_;
        std::string id_;
    };
} // namespace transactions
} // namespace couchbase
