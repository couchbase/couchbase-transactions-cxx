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

#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "uid_generator.hxx"

namespace uuids = boost::uuids;
namespace tx = couchbase::transactions;

std::mutex tx::uid_generator::mutex_;

std::string
tx::uid_generator::next()
{
    std::lock_guard<std::mutex> lock(mutex_);
    static auto generator = uuids::random_generator();
    return uuids::to_string(generator());
}
