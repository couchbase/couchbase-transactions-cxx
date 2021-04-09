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

/**
 * @file
 *
 * Provides functionality for subdoc lookup operations in Couchbase Transactions Client
 */

namespace couchbase
{
class collection;

enum class lookup_in_spec_type { LOOKUP_IN_GET, LOOKUP_IN_FULLDOC_GET };
/**
 * @brief Specify specific elements in a document to look up
 *
 * Used in @ref collection::lookup_in, you can specify a specific path
 * within a document to return.  See @ref collection::lookup_in.
 *
 */
class lookup_in_spec
{
    friend collection;

  public:
    /**
     * Get everything at a specific path.
     *
     * @param path The dot-separated path you are interested in.
     * @return A new lookup_in_spec.
     */
    static lookup_in_spec get(const std::string& path)
    {
        return lookup_in_spec(lookup_in_spec_type::LOOKUP_IN_GET, path);
    }

    /**
     * Get entire doc.
     *
     * @return A new lookup_in_spec.
     */
    static lookup_in_spec fulldoc_get()
    {
        return lookup_in_spec(lookup_in_spec_type::LOOKUP_IN_FULLDOC_GET);
    }

    /**
     *  Specify the lookup is on xattrs, rather than the document body.
     *
     * @return Reference to the spec, so you can chain the calls.
     */
    lookup_in_spec& xattr();

  private:
    lookup_in_spec_type type_;
    std::string path_;
    uint32_t flags_;

    explicit lookup_in_spec(lookup_in_spec_type type, std::string path = "")
      : type_(type)
      , path_(std::move(path))
      , flags_(0)
    {
    }
};
} // namespace couchbase
