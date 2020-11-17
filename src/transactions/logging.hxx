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
#include <couchbase/transactions/attempt_context.hxx>
#include <spdlog/fmt/ostr.h>
#include <spdlog/spdlog.h>

static const std::string format_string("[{}/{}]:");

template<typename... Args>
void
trace(couchbase::transactions::attempt_context& ctx, const std::string& fmt, Args... args)
{
    spdlog::trace(format_string + fmt, ctx.transaction_id(), ctx.id(), args...);
}

template<typename... Args>
void
info(couchbase::transactions::attempt_context& ctx, const std::string& fmt, Args... args)
{
    spdlog::info(format_string + fmt, ctx.transaction_id(), ctx.id(), args...);
}

template<typename... Args>
void
error(couchbase::transactions::attempt_context& ctx, const std::string& fmt, Args... args)
{
    spdlog::error(format_string + fmt, ctx.transaction_id(), ctx.id(), args...);
}
