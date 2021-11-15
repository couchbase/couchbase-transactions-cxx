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
#include "logging.hxx"
#include <condition_variable>
#include <mutex>

namespace couchbase::transactions
{

class async_operation_conflict : public std::runtime_error
{
  public:
    async_operation_conflict(const std::string& msg)
      : std::runtime_error(msg)
    {
    }
};

class waitable_op_list
{
  public:
    waitable_op_list()
      : count_(0)
      , allow_ops_(true)
    {
    }

    void wait_and_block_ops()
    {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock, [this]() { return (0 == count_); });
        // we have the lock.  Block all further ops
        allow_ops_ = false;
    }
    void change_count(int32_t val)
    {
        std::lock_guard<std::mutex> lock(mutex_);
        if (allow_ops_) {
            count_ += val;
            txn_log->trace("op count changed by {} to {}", val, count_);
            assert(count_ >= 0);
            if (0 == count_) {
                cond_.notify_all();
            }
        } else {
            txn_log->error("operation attempted after commit/rollback");
            throw async_operation_conflict("Operation attempted after commit or rollback");
        }
    }

  private:
    int32_t count_;
    bool allow_ops_;
    std::condition_variable cond_;
    std::mutex mutex_;
};
}; // namespace couchbase::transactions
