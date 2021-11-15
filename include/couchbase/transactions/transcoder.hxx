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
#include <couchbase/internal/nlohmann/json.hpp>
#include <memory>
#include <string>
#include <utility>

namespace couchbase
{
namespace transactions
{

    class encoding_error : public std::runtime_error
    {
      public:
        encoding_error(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };

    class decoding_error : public std::runtime_error
    {
      public:
        decoding_error(const std::string& what)
          : std::runtime_error(what)
        {
        }
    };

    namespace default_json_serializer
    {
        template<typename T, typename std::enable_if<std::is_same<T, std::string>::value>::type* = nullptr>
        T deserialize(const std::string& json_string)
        {
            return json_string;
        }
        template<typename T, typename std::enable_if<std::is_same<T, nlohmann::json>::value>::type* = nullptr>
        T deserialize(const std::string& json_string)
        {
            return nlohmann::json::parse(json_string);
        }
        template<
          typename T,
          typename std::enable_if<(!std::is_same<T, std::string>::value) && (!std::is_same<T, nlohmann::json>::value)>::type* = nullptr>
        T deserialize(const std::string& json_string)
        {
            return nlohmann::json::parse(json_string).get<T>();
        }
        template<typename T>
        T deserialize_from_json_string(const std::string& json_string)
        {
            return deserialize<nlohmann::json>(json_string).get<T>();
        }

        template<typename T, typename std::enable_if<std::is_same<T, std::string>::value>::type* = nullptr>
        std::string serialize(const T& obj)
        {
            return obj;
        }
        template<typename T, typename std::enable_if<std::is_same<T, nlohmann::json>::value>::type* = nullptr>
        std::string serialize(const T& obj)
        {
            return obj.dump();
        }
        template<
          typename T,
          typename std::enable_if<(!std::is_same<T, std::string>::value) && (!std::is_same<T, nlohmann::json>::value)>::type* = nullptr>
        std::string serialize(const T& obj)
        {
            nlohmann::json j = obj;
            return j.dump();
        }

    } // namespace default_json_serializer
} // namespace transactions
} // namespace couchbase
