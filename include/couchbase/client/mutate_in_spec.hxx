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

#include <string>

#include <couchbase/internal/nlohmann/json.hpp>

/**
 * @file
 *
 * Provides functionality for subdoc mutation operations in Couchbase Transactions Client
 */

namespace couchbase
{
class collection;

enum class mutate_in_spec_type { MUTATE_IN_UPSERT, MUTATE_IN_INSERT, MUTATE_IN_FULLDOC_INSERT, MUTATE_IN_FULLDOC_UPSERT, REMOVE };

/**
 * @brief Useful macros
 *
 * When mutating a document, if you need to store the cas of the document, the sequence number, or a crc of it,
 * you can use these macros and the server does it for you when it stores the mutation.  The server calculates
 * these after all the mutations in the operation.
 */
namespace mutate_in_macro
{
    /** @brief this expands to be the current document CAS (after the mutation) */
    static const std::string CAS = "${Mutation.CAS}";
    /** @brief this macro expands to the current sequence number (rev) of the document (after the mutation)*/
    static const std::string SEQ_NO = "${Mutation.seqno}";
    /** @brief this macro expands to a CRC32C of the document (after the mutation)*/
    static const std::string VALUE_CRC_32C = "${Mutation.value_crc32c}";
}; // namespace mutate_in_macro

/**
 * @brief Specify specific elements in a document to mutate
 *
 * Used in @ref collection::mutate_in, you can specify a specific path
 * within a document to insert, remove, upsert.  See @ref collection::lookup_in.
 *
 * You can pass in a primitive, like int, char*, etc..., std::string, a nlohmann::json
 * object, or your own object which has to_json and from_json implemented.
 * See https://github.com/nlohmann/json#basic-usage
 *
 */
class mutate_in_spec
{
    friend collection;

  public:
    /**
     * @brief  Upsert content at a path within a document.
     *
     * Either inserts the content, or replaces existing content at this path.
     *
     * @param path A dot-separated string representing the path to mutate.
     * @param value The content to place at that path.
     * @return The newly created mutate_in_spec.
     */
    template<typename Content>
    static mutate_in_spec upsert(const std::string& path, const Content& value)
    {
        return mutate_in_spec(mutate_in_spec_type::MUTATE_IN_UPSERT, path, value);
    }

    /**
     *  @brief Insert content at a path within a document.
     *
     * Expects there to be no content at this path.
     *
     * @param path A dot-separated string representing the path to mutate.
     * @param value The content to place at that path.
     * @return The newly created mutate_in_spec.
     */
    template<typename Content>
    static mutate_in_spec insert(const std::string& path, const Content& value)
    {
        return mutate_in_spec(mutate_in_spec_type::MUTATE_IN_INSERT, path, value);
    }

    /**
     * @brief Inserts the content as the entire body of the document
     *
     * This is an insert - expecting there to be no document already there.
     *
     * @param value The content to place at that path.
     * @return The newly created mutate_in_spec.
     */
    template<typename Content>
    static mutate_in_spec fulldoc_insert(const Content& value)
    {
        return mutate_in_spec(mutate_in_spec_type::MUTATE_IN_FULLDOC_INSERT, value);
    }

    /**
     * @brief Upsert the content as the entire body of the document
     *
     * Either replaces an existing document, or inserts a new one with the content provided.
     *
     * @param value The content to place at that path.
     * @return The newly created mutate_in_spec.
     */
    template<typename Content>
    static mutate_in_spec fulldoc_upsert(const Content& value)
    {
        return mutate_in_spec(mutate_in_spec_type::MUTATE_IN_FULLDOC_UPSERT, value);
    }

    /**
     * @brief Remove content at a path within a document.
     *
     * @param path A dot-separated string representing the path to remove.
     * @return The newly created mutate_in_spec.
     */
    static mutate_in_spec remove(const std::string& path)
    {
        return mutate_in_spec(mutate_in_spec_type::REMOVE, path, std::string(""));
    }

    /**
     * @brief Specify the mutation is on xattrs, rather than the document body.
     *
     * @return Reference to the spec, so you can chain the calls.
     */
    mutate_in_spec& xattr();

    /**
     * @brief Specify the mutation creates the entire path.
     *
     * @return Reference to the spec, so you can chain the calls.
     */
    mutate_in_spec& create_path();

    /**
     * @brief Specify the value in this mutation spec contains a @ref mutate_in_macro.
     *
     * @return Reference to the spec, so you can chain the calls.
     */
    mutate_in_spec& expand_macro();

  private:
    mutate_in_spec_type type_;
    std::string path_;
    std::string value_;
    uint32_t flags_;

    mutate_in_spec(mutate_in_spec_type type, std::string path, const nlohmann::json& value)
      : type_(type)
      , path_(std::move(path))
      , value_(value.dump())
      , flags_(0)
    {
    }
    mutate_in_spec(mutate_in_spec_type type, const nlohmann::json& value)
      : type_(type)
      , path_("")
      , value_(value.dump())
      , flags_(0)
    {
    }
};

} // namespace couchbase
