/*
 *     Copyright 2022 Couchbase, Inc.
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
#include "transactions_env.h"
#include <couchbase/transactions/internal/logging.hxx>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <iostream>
#include <iterator>
#include <spdlog/fwd.h>
#include <spdlog/sinks/base_sink.h>

using namespace couchbase::transactions;

auto doc_content = nlohmann::json::parse("{\"some\": \"thing\"}");

class TrivialFileSink : public spdlog::sinks::base_sink<std::mutex>
{
  protected:
    void sink_it_(const spdlog::details::log_msg& msg) override
    {
        spdlog::memory_buf_t formatted;
        base_sink<std::mutex>::formatter_->format(msg, formatted);
        std::cerr << formatted.data();
    }
    void flush_() override
    {
    }

  private:
};

TEST(LoggingTests, CanLogToStdoutByDefault)
{
    couchbase::core::logger::create_blackhole_logger();
    std::string log_message = "I am a log";
    couchbase::transactions::set_transactions_log_level(couchbase::core::logger::level::debug);
    testing::internal::CaptureStdout();
    txn_log->debug(log_message);
    txn_log->flush();
    auto out = testing::internal::GetCapturedStdout();
    ASSERT_FALSE(out.empty());
    ASSERT_NE(std::string::npos, out.find(log_message));
    couchbase::core::logger::create_console_logger();
}

TEST(LoggingTests, LogLevelsWork)
{
    couchbase::core::logger::create_blackhole_logger();
    std::string log_message = "I am a log";
    couchbase::transactions::set_transactions_log_level(couchbase::core::logger::level::info);
    testing::internal::CaptureStdout();
    txn_log->debug(log_message);
    txn_log->flush();
    auto out = testing::internal::GetCapturedStdout();
    ASSERT_TRUE(out.empty());
    couchbase::core::logger::create_console_logger();
}

TEST(LoggingTests, CanUseCustomSink)
{
    couchbase::core::logger::create_blackhole_logger();
    std::string log_message = "I am a log";
    auto sink = std::make_shared<TrivialFileSink>();
    couchbase::transactions::create_loggers(couchbase::core::logger::level::debug, sink);
    testing::internal::CaptureStdout();
    testing::internal::CaptureStderr();
    txn_log->debug(log_message);
    txn_log->flush();
    auto out = testing::internal::GetCapturedStdout();
    ASSERT_TRUE(out.empty()) << "out = " << out;
    auto err = testing::internal::GetCapturedStderr();
    ASSERT_FALSE(err.empty());
    ASSERT_NE(std::string::npos, err.find(log_message)) << "err = " << err;
    couchbase::core::logger::create_console_logger();
}

TEST(LoggingTests, CustomSinkRespectsLogLevels)
{
    couchbase::core::logger::create_blackhole_logger();
    std::string log_message = "I am a log";
    auto sink = std::make_shared<TrivialFileSink>();
    couchbase::transactions::create_loggers(couchbase::core::logger::level::info, sink);
    testing::internal::CaptureStdout();
    testing::internal::CaptureStderr();
    txn_log->debug(log_message);
    txn_log->flush();
    auto out = testing::internal::GetCapturedStdout();
    ASSERT_TRUE(out.empty()) << "out = " << out;
    auto err = testing::internal::GetCapturedStderr();
    ASSERT_TRUE(err.empty()) << "err = " << err;
    couchbase::core::logger::create_console_logger();
}

TEST(LoggingTests, CustomSinkRespectsLogLevelChanges)
{
    couchbase::core::logger::create_blackhole_logger();
    std::string log_message = "I am a log";
    auto sink = std::make_shared<TrivialFileSink>();
    couchbase::transactions::create_loggers(couchbase::core::logger::level::debug, sink);
    couchbase::transactions::set_transactions_log_level(couchbase::core::logger::level::info);
    testing::internal::CaptureStdout();
    testing::internal::CaptureStderr();
    txn_log->debug(log_message);
    txn_log->flush();
    auto out = testing::internal::GetCapturedStdout();
    ASSERT_TRUE(out.empty()) << "out = " << out;
    auto err = testing::internal::GetCapturedStderr();
    ASSERT_TRUE(err.empty()) << "err = " << err;
    couchbase::core::logger::create_console_logger();
}
