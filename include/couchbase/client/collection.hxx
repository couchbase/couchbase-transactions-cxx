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

#include <couchbase/client/bucket.hxx>
#include <couchbase/client/lookup_in_spec.hxx>
#include <couchbase/client/mutate_in_spec.hxx>
#include <couchbase/client/result.hxx>
#include <couchbase/support.hxx>
#include <couchbase/client/options.hxx>
#include <string>
#include <vector>

namespace couchbase
{

enum class store_operation { upsert, insert, replace };

result
store_impl(collection* coll, store_operation op, const std::string& id, const std::string& payload, uint64_t cas, durability_level level);

class collection
{
    friend class bucket;

  private:
    std::string scope_;
    std::string name_;
    std::shared_ptr<bucket> bucket_;
    std::string bucket_name_;

    friend result store_impl(collection* coll,
                             store_operation op,
                             const std::string& id,
                             const std::string& payload,
                             uint64_t cas,
                             durability_level level);

    lcb_st* lcb()
    {
        return bucket_->lcb_;
    }

    template<typename Content>
    result store(store_operation operation, const std::string& id, const Content& value, uint64_t cas, durability_level level)
    {
        nlohmann::json j = value;
        std::string payload = j.dump();
        return store_impl(this, operation, id, payload, cas, level);
    }

  public:
    explicit collection(std::shared_ptr<bucket> bucket, std::string scope, std::string name);

    result get(const std::string& id, const get_options& opts = get_options());

    template<typename Content>
    result upsert(const std::string& id, const Content& value, const upsert_options& opts = upsert_options())
    {
        return store(store_operation::upsert, id, value, opts.cas().value_or(0), opts.durability().value_or(durability_level::none));
    }

    template<typename Content>
    result insert(const std::string& id, const Content& value, const insert_options& opts = insert_options())
    {
        return store(store_operation::insert, id, value, 0, opts.durability().value_or(durability_level::none));
    }

    template<typename Content>
    result replace(const std::string& id, const Content& value, const replace_options& opts = replace_options())
    {
        return store(store_operation::replace, id, value, opts.cas().value_or(0), opts.durability().value_or(durability_level::none));
    }

    result remove(const std::string& id, const remove_options& opts = remove_options());

    result mutate_in(const std::string& id, std::vector<mutate_in_spec> specs, const mutate_in_options& opts = mutate_in_options());

    result lookup_in(const std::string& id, std::vector<lookup_in_spec> specs, const lookup_in_options& opts = lookup_in_options());

    CB_NODISCARD const std::string& name() const
    {
        return name_;
    }

    CB_NODISCARD const std::string& scope() const
    {
        return scope_;
    }

    CB_NODISCARD const std::string& bucket_name() const
    {
        return bucket_name_;
    }
};
} // namespace couchbase
