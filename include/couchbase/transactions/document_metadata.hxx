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

#include <cstdint>
#include <string>

#include <boost/optional.hpp>

namespace couchbase
{
namespace transactions
{
    /**
     * @brief Stores some $document metadata from when the document is fetched
     */
    class document_metadata
    {
      public:
        /**
         *  @internal
         *  @brief Create document metadata, given results of a kv operation
         *
         *  We expect this constructor to become private soon.
         */
        document_metadata(boost::optional<std::string> cas,
                          boost::optional<std::string> revid,
                          boost::optional<std::uint32_t> exptime,
                          boost::optional<std::string> crc32)
          : cas_(std::move(cas))
          , revid_(std::move(revid))
          , exptime_(exptime)
          , crc32_(crc32)
        {
        }

        /**
         * @brief Get CAS for the document
         *
         * @return the CAS of the document, as a string.
         */
        CB_NODISCARD boost::optional<std::string> cas() const
        {
            return cas_;
        }

        /**
         * @brief Get revid for the document
         *
         * @return the revid of the document, as a string.
         */
        CB_NODISCARD boost::optional<std::string> revid() const
        {
            return revid_;
        }

        /**
         * @brief Get the expiry of the document, if set
         *
         * @return the expiry of the document, if one was set, and the request
         *         specified it.
         */
        CB_NODISCARD boost::optional<std::uint32_t> exptime() const
        {
            return exptime_;
        }

        /**
         * @brief Get the crc for the document
         *
         * @return the crc-32 for the document, as a string
         */
        CB_NODISCARD boost::optional<std::string> crc32() const
        {
            return crc32_;
        }

      private:
        const boost::optional<std::string> cas_;
        const boost::optional<std::string> revid_;
        const boost::optional<std::uint32_t> exptime_;
        const boost::optional<std::string> crc32_;
    };
} // namespace transactions
} // namespace couchbase
