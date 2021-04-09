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

#include <mutex>
#include <string>
#include <vector>

#include "attempt_context_impl.hxx"
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

        CB_NODISCARD const staged_mutation_type& type() const
        {
            return type_;
        }

        void type(staged_mutation_type& type)
        {
            type_ = type;
        }

        template<typename Content>
        const Content& content() const
        {
            return content_;
        }

        template<typename Content>
        void content(Content& content)
        {
            content_ = content;
        }
    };

    class staged_mutation_queue
    {
      private:
        std::mutex mutex_;
        std::vector<staged_mutation> queue_;
        void commit_doc(attempt_context_impl& ctx,
                        staged_mutation& item,
                        bool ambiguity_resolution_mode = false,
                        bool cas_zero_mode = false);
        void remove_doc(attempt_context_impl& ctx, staged_mutation& item);
        void rollback_insert(attempt_context_impl& ctx, staged_mutation& item);
        void rollback_remove_or_replace(attempt_context_impl& ctx, staged_mutation& item);

      public:
        bool empty();
        void add(const staged_mutation& mutation);
        void extract_to(const std::string& prefix, std::vector<couchbase::mutate_in_spec>& specs);
        void commit(attempt_context_impl& ctx);
        void rollback(attempt_context_impl& ctx);
        void iterate(std::function<void(staged_mutation&)>);

        staged_mutation* find_replace(std::shared_ptr<collection> collection, const std::string& id);
        staged_mutation* find_insert(std::shared_ptr<collection> collection, const std::string& id);
        staged_mutation* find_remove(std::shared_ptr<collection> collection, const std::string& id);
    };
} // namespace transactions
} // namespace couchbase
