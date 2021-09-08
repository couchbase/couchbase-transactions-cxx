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

#include <functional>
#include <string>

namespace couchbase
{
namespace transactions
{
    namespace
    {
        int noop1(const std::string&)
        {
            return 1;
        }
        int noop2()
        {
            return 1;
        }
        void noop3()
        {
            return;
        }
    } // namespace

    /**
     * Hooks purely for testing purposes.  If you're an end-user looking at these for any reason, then please contact us first
     * about your use-case: we are always open to adding good ideas into the transactions library.
     */
    struct cleanup_testing_hooks {
        std::function<int(const std::string& id)> before_commit_doc = noop1;
        std::function<int(const std::string& id)> before_doc_get = noop1;
        std::function<int(const std::string& id)> before_remove_doc_staged_for_removal = noop1;
        std::function<int(const std::string& id)> before_remove_doc = noop1;
        std::function<int(const std::string& id)> before_atr_get = noop1;
        std::function<int(const std::string& id)> before_remove_links = noop1;

        std::function<int(void)> before_atr_remove = noop2;

        std::function<void(void)> on_cleanup_docs_completed = noop3;
        std::function<void(void)> on_cleanup_completed = noop3;

        std::function<int(const std::string&)> client_record_before_create = noop1;
        std::function<int(const std::string&)> client_record_before_get = noop1;
        std::function<int(const std::string&)> client_record_before_update = noop1;
        std::function<int(const std::string&)> client_record_before_remove_client = noop1;
    };
} // namespace transactions
} // namespace couchbase
