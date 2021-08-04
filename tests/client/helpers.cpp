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
#include "helpers.hxx"
bool
operator==(const SimpleObject& lhs, const SimpleObject& rhs)
{
    return (rhs.name == lhs.name) && (rhs.number == lhs.number);
}

void
to_json(nlohmann::json& j, const SimpleObject& o)
{
    j = nlohmann::json{ { "name", o.name }, { "number", o.number } };
}

void
from_json(const nlohmann::json& j, SimpleObject& o)
{
    j.at("name").get_to(o.name);
    j.at("number").get_to(o.number);
}

bool
operator==(const AnotherSimpleObject& lhs, const AnotherSimpleObject& rhs)
{
    return lhs.foo == rhs.foo;
}

void
to_json(nlohmann::json& j, const AnotherSimpleObject& o)
{
    j = nlohmann::json{ { "foo", o.foo } };
}

void
from_json(const nlohmann::json& j, AnotherSimpleObject& o)
{
    j.at("foo").get_to(o.foo);
}
