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

#include <chrono>
#include <couchbase/client/bucket.hxx>
#include <couchbase/client/lookup_in_spec.hxx>
#include <couchbase/client/mutate_in_spec.hxx>
#include <couchbase/client/options.hxx>
#include <couchbase/client/result.hxx>
#include <couchbase/support.hxx>
#include <string>
#include <thread>
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
    std::weak_ptr<bucket> bucket_;

    friend result store_impl(collection* coll,
                             store_operation op,
                             const std::string& id,
                             const std::string& payload,
                             uint64_t cas,
                             durability_level level);

    template<typename Content>
    result store(store_operation operation, const std::string& id, const Content& value, uint64_t cas, durability_level level)
    {
        nlohmann::json j = value;
        std::string payload = j.dump();
        return store_impl(this, operation, id, payload, cas, level);
    }

    std::unique_ptr<pool<lcb_st*>>& instance_pool()
    {
        return bucket_.lock()->instance_pool_;
    }
    result wrap_call_for_retry(std::function<result(void)> fn);

    collection(std::shared_ptr<bucket> bucket, std::string scope, std::string name);

    static void install_callbacks(lcb_st* lcb);

  public:
    /**
     *  @brief Get a document by key
     *
     * Returns a @ref result containing document if a document with that key exists, otherwise
     * the @ref result will contain the error.
     *
     * @param id Key of document to get.
     * @param opts Options to use for this command.
     * @return result The result of the operation.  See @ref result.
     */
    result get(const std::string& id, const get_options& opts = get_options());

    /**
     *
     */
    result exists(const std::string& id, const exists_options& opts = exists_options());

    /**
     * @brief Upsert document
     *
     * Inserts a new document or replaces an existing document, with the Content given.
     *
     * @param id Key of document to upsert.
     * @param value The document itself.  Note that the object either needs to be an nlohmann::json object or
     *              there needs to be to/from_json functions defined for it.
     * @param opts Options to use when upserting.  For instance, you can set a durability.  See @ref upsert_options.
     * @return Result of the operation.
     */
    template<typename Content>
    result upsert(const std::string& id, const Content& value, const upsert_options& opts = upsert_options())
    {
        return wrap_call_for_retry([&]() -> result {
            return store(store_operation::upsert, id, value, opts.cas().value_or(0), opts.durability().value_or(durability_level::none));
        });
    }

    /**
     * @brief Insert document
     *
     * Inserts a new document with the Content given.
     *
     * @param id Key of document to insert.
     * @param value The document itself.  Note that the object either needs to be an nlohmann::json object or
     *              there needs to be to/from_json functions defined for it.
     * @param opts Options to use when inserting.  For instance, you can set a durability.  See @ref insert_options.
     * @return Result of the operation.
     */
    template<typename Content>
    result insert(const std::string& id, const Content& value, const insert_options& opts = insert_options())
    {
        return wrap_call_for_retry(
          [&]() -> result { return store(store_operation::insert, id, value, 0, opts.durability().value_or(durability_level::none)); });
    }

    /**
     * @brief Replace document
     *
     * Replaces an existing document with the Content given.
     *
     * @param id Key of document to replace.
     * @param value The document itself.  Note that the object either needs to be an nlohmann::json object or
     *              there needs to be to/from_json functions defined for it.
     * @param opts Options to use when replacing.  For instance, you can set a durability or cas.  See
     *             @ref replace_options.
     * @return Result of the operation.
     */
    template<typename Content>
    result replace(const std::string& id, const Content& value, const replace_options& opts = replace_options())
    {
        return wrap_call_for_retry([&]() -> result {
            return store(store_operation::replace, id, value, opts.cas().value_or(0), opts.durability().value_or(durability_level::none));
        });
    }

    /**
     * @brief Remove document
     *
     * Removes an existing document.
     *
     * @param id Key of document to remove.
     * @param opts Options to use when removing.  For instance, you can set a durability or cas.  See
     *             @ref remove_options.
     * @return Result of the operation.
     */
    result remove(const std::string& id, const remove_options& opts = remove_options());

    /**
     * @brief Mutate some elements of a document.
     *
     * Mutates some paths within a document.  See @ref mutate_in_specs for the various possibilities and limitations. Useful
     * avoiding constructing, sending entire document, if all you want to do is modify a small fraction of it.
     *
     * @param id Key of doc to mutate.
     * @param specs Vector of specs that represent the mutations.
     * @param opts Options to use when mutating.  You can specify a durability or cas, for instance.  See @ref mutate_in_options.
     * @return Result of operation.
     */
    result mutate_in(const std::string& id, std::vector<mutate_in_spec> specs, const mutate_in_options& opts = mutate_in_options());

    /**
     * @brief Lookup some elements of a document.
     *
     * Lookup some elements in a document.  See @ref lookup_in_specs for the various possibilities and limitations. Useful
     * when you don't want to fetch and parse entire document.
     *
     * @param id Key of doc to mutate.
     * @param specs Vector of specs that represent the mutations.
     * @param opts Options to use when mutating.  You can specify a durability or cas, for instance.  See @ref mutate_in_options.
     * @return Result of operation.
     */
    result lookup_in(const std::string& id, std::vector<lookup_in_spec> specs, const lookup_in_options& opts = lookup_in_options());

    /**
     * @brief Get name of collection
     *
     *  @return Name of collection.  Note the default collection is named "_default".
     */
    CB_NODISCARD const std::string& name() const
    {
        return name_;
    }

    /**
     * @brief Name of scope for this collection
     *
     * @return Scope of collection.  Note default scope is "_default".
     */
    CB_NODISCARD const std::string& scope() const
    {
        return scope_;
    }

    /**
     * @brief Get name of bucket for this collection
     *
     * @return Name of bucket for this collection.
     */
    CB_NODISCARD const std::string& bucket_name() const
    {
        return bucket_.lock()->name();
    }

    /**
     * @brief Get bucket for this collection.
     *
     * @return @ref bucket for this collection.
     */
    CB_NODISCARD std::shared_ptr<couchbase::bucket> get_bucket()
    {
        return bucket_.lock();
    }

};
} // namespace couchbase
