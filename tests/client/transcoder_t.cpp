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

#include "../client/helpers.hxx"
#include <couchbase/client/transcoder.hxx>
#include <gtest/gtest.h>

using namespace couchbase;

TEST(DefaultTranscoderTests, ObjectRoundTrip)
{
    SimpleObject o{ "some name", 100 };
    auto encoded = couchbase::default_json_serializer::serialize(o);
    SimpleObject decoded = couchbase::default_json_serializer::deserialize<SimpleObject>(encoded);
    ASSERT_EQ(o, decoded);
}

TEST(TranscoderTests, NLohmannJsonRoundTrip)
{
    nlohmann::json o{ "some name", 100 };
    ASSERT_EQ(o, couchbase::default_json_serializer::deserialize<nlohmann::json>(couchbase::default_json_serializer::serialize(o)));
}

TEST(DefaultTranscoderTests, StringsRoundTrip)
{
    std::string json_str("{\"name\": \"someone\", \"number\": 100}");
    ASSERT_EQ(default_json_serializer::deserialize<std::string>(json_str), json_str);
}

TEST(DefaultTranscoderTests, DeserializesAsExpected)
{
    std::string json_str("{\"name\": \"someone\", \"number\": 100}");
    auto parsed = nlohmann::json::parse(json_str);
    SimpleObject o{ "someone", 100 };
    ASSERT_EQ(parsed, default_json_serializer::deserialize<nlohmann::json>(json_str));
    ASSERT_EQ(parsed, nlohmann::json::parse(default_json_serializer::deserialize<std::string>(json_str)));
    ASSERT_EQ(o, default_json_serializer::deserialize<SimpleObject>(json_str));
}

TEST(DefaultTranscoerTests, SerializesAsExpected)
{
    // construct object, then nlohmann::json and string from it directly
    SimpleObject o{ "someone", 100 };
    nlohmann::json parsed = o;
    auto json_string = parsed.dump();

    // use parsed string to compare parsed serializer results for consistency
    auto json_obj = nlohmann::json::parse(json_string);

    ASSERT_EQ(json_obj, nlohmann::json::parse(default_json_serializer::serialize(o)));
    ASSERT_EQ(json_obj, nlohmann::json::parse(default_json_serializer::serialize(parsed)));
    ASSERT_EQ(json_obj, nlohmann::json::parse(default_json_serializer::serialize(json_string)));
}
