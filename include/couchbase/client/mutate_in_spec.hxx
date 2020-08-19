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

#include <string>

#include <nlohmann/json.hpp>

namespace couchbase
{
class collection;

enum class mutate_in_spec_type { MUTATE_IN_UPSERT, MUTATE_IN_INSERT, MUTATE_IN_FULLDOC_INSERT, MUTATE_IN_FULLDOC_UPSERT, REMOVE };

namespace mutate_in_macro
{
    static const std::string CAS = "${Mutation.CAS}";
    static const std::string SEQ_NO = "${Mutation.seqno}";
    static const std::string VALUE_CRC_32C = "${Mutation.value_crc32c}";
}; // namespace mutate_in_macro

class mutate_in_spec
{
    friend collection;

  public:
    template<typename Content>
    static mutate_in_spec upsert(const std::string& path, const Content& value)
    {
        return mutate_in_spec(mutate_in_spec_type::MUTATE_IN_UPSERT, path, value);
    }

    template<typename Content>
    static mutate_in_spec insert(const std::string& path, const Content& value)
    {
        return mutate_in_spec(mutate_in_spec_type::MUTATE_IN_INSERT, path, value);
    }

    template<typename Content>
    static mutate_in_spec fulldoc_insert(const Content& value)
    {
        return mutate_in_spec(mutate_in_spec_type::MUTATE_IN_FULLDOC_INSERT, value);
    }

    template<typename Content>
    static mutate_in_spec fulldoc_upsert(const Content& value)
    {
        return mutate_in_spec(mutate_in_spec_type::MUTATE_IN_FULLDOC_UPSERT, value);
    }

    static mutate_in_spec remove(const std::string& path)
    {
        return mutate_in_spec(mutate_in_spec_type::REMOVE, path, std::string(""));
    }

    mutate_in_spec& xattr();

    mutate_in_spec& create_path();

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
