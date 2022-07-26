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

#include <core/document_id.hxx>
#include <string>

namespace couchbase::transactions
{
/**
 * @brief  Offline, serializable representation of a bucket, scope, and collection
 */
struct transaction_keyspace {
    std::string bucket;
    std::string scope{ "_default" };
    std::string collection{ "_default" };

    transaction_keyspace(const std::string& bucket_name, const std::string& scope_name, const std::string& collection_name)
      : bucket(bucket_name)
      , scope(scope_name)
      , collection(collection_name)
    {
        if (scope.empty()) {
            scope = "_default";
        }
        if (collection.empty()) {
            collection = "_default";
        }
    }

    transaction_keyspace(const std::string& bucket_name)
      : transaction_keyspace{ bucket_name, "_default", "_default" }
    {
    }

    transaction_keyspace(const core::document_id& id)
      : bucket(id.bucket())
      , scope(id.scope())
      , collection(id.collection())
    {
    }
};
} // namespace couchbase::transactions