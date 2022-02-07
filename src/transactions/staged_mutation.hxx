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
#include "couchbase/transactions/internal/utils.hxx"
#include <couchbase/transactions/transaction_get_result.hxx>

namespace couchbase
{
namespace transactions
{
    enum class staged_mutation_type { INSERT, REMOVE, REPLACE };

    class staged_mutation
    {
      private:
        transaction_get_result doc_;
        staged_mutation_type type_;
        std::string content_;

      public:
        template<typename Content>
        staged_mutation(transaction_get_result& doc, Content content, staged_mutation_type type)
          : doc_(std::move(doc))
          , type_(type)
          , content_(std::move(content))
        {
        }

        staged_mutation(const staged_mutation& o) = default;

        CB_NODISCARD staged_mutation& operator=(const staged_mutation& o)
        {
            doc_ = o.doc_;
            type_ = o.type_;
            content_ = o.content_;
            return *this;
        }

        CB_NODISCARD const couchbase::document_id& id() const
        {
            return doc_.id();
        }

        transaction_get_result& doc()
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

        const std::string& content() const
        {
            return content_;
        }

        void content(const std::string& content)
        {
            content_ = content;
        }

        std::string type_as_string() const
        {
            switch (type_) {
                case staged_mutation_type::INSERT:
                    return "INSERT";
                case staged_mutation_type::REMOVE:
                    return "REMOVE";
                case staged_mutation_type::REPLACE:
                    return "REPLACE";
            }
            throw std::runtime_error("unknown type of staged mutation");
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
        void extract_to(const std::string& prefix, couchbase::operations::mutate_in_request& req);
        void commit(attempt_context_impl& ctx);
        void rollback(attempt_context_impl& ctx);
        void iterate(std::function<void(staged_mutation&)>);
        void remove_any(const couchbase::document_id& id);

        staged_mutation* find_any(const couchbase::document_id& id);
        staged_mutation* find_replace(const couchbase::document_id& id);
        staged_mutation* find_insert(const couchbase::document_id& id);
        staged_mutation* find_remove(const couchbase::document_id& id);
    };
} // namespace transactions
} // namespace couchbase
