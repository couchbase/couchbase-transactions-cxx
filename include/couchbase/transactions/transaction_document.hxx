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

#include <ostream>

#include <boost/algorithm/string/split.hpp>

#include <couchbase/internal/nlohmann/json.hpp>

#include <couchbase/client/collection.hxx>
#include <couchbase/transactions/document_metadata.hxx>
#include <couchbase/transactions/transaction_document_status.hxx>
#include <couchbase/transactions/transaction_links.hxx>
#include <spdlog/fmt/ostr.h>
#include <utility>

namespace couchbase
{
namespace transactions
{
    class transaction_document
    {
      private:
        collection& collection_;
        nlohmann::json value_;
        std::string id_;
        uint64_t cas_;
        transaction_links links_;
        transaction_document_status status_;

        /** This is needed for provide {BACKUP-FIELDS}.  It is only needed from the get to the staged mutation, hence Optional. */
        const boost::optional<document_metadata> metadata_;

      public:
        transaction_document(const transaction_document& doc)
          : collection_(doc.collection_)
          , value_(doc.value_)
          , id_(doc.id_)
          , links_(doc.links_)
          , status_(doc.status_)
          , metadata_(doc.metadata_)
          , cas_(doc.cas_)
        {
        }

        template<typename Content>
        transaction_document(std::string id,
                             Content content,
                             uint64_t cas,
                             collection& collection,
                             transaction_links links,
                             transaction_document_status status,
                             boost::optional<document_metadata> metadata)
          : id_(std::move(id))
          , cas_(cas)
          , collection_(collection)
          , links_(std::move(links))
          , status_(status)
          , metadata_(std::move(metadata))
          , value_(std::move(content))
        {
        }

        template<typename Content>
        static transaction_document create_from(transaction_document& document, Content content, transaction_document_status status)
        {
            // TODO: just copy it instead
            transaction_links links(document.links().atr_id(),
                                    document.links().atr_bucket_name(),
                                    document.links().atr_scope_name(),
                                    document.links().atr_collection_name(),
                                    document.links().staged_transaction_id(),
                                    document.links().staged_attempt_id(),
                                    document.links().staged_content<Content>(),
                                    document.links().cas_pre_txn(),
                                    document.links().revid_pre_txn(),
                                    document.links().exptime_pre_txn(),
                                    document.links().crc32_of_staging(),
                                    document.links().op());

            return transaction_document(
              document.id(), content, document.cas(), document.collection_ref(), links, status, document.metadata());
        }

        static transaction_document create_from(collection& collection, std::string id, result res, transaction_document_status status)
        {
            spdlog::trace("creating doc from {}", res);
            boost::optional<std::string> atr_id;
            boost::optional<std::string> transaction_id;
            boost::optional<std::string> attempt_id;
            boost::optional<nlohmann::json> staged_content;
            boost::optional<std::string> atr_bucket_name;
            boost::optional<std::string> atr_scope_name;
            boost::optional<std::string> atr_collection_name;

            // read from xattrs.txn.restore
            boost::optional<std::string> cas_pre_txn;
            boost::optional<std::string> revid_pre_txn;
            boost::optional<uint32_t> exptime_pre_txn;
            boost::optional<std::string> crc32_of_staging;

            // read from $document
            boost::optional<std::string> cas_from_doc;
            boost::optional<std::string> revid_from_doc;
            boost::optional<uint32_t> exptime_from_doc;
            boost::optional<std::string> crc32_from_doc;

            boost::optional<std::string> op;
            nlohmann::json content;

            if (res.values[0]) {
                atr_id = res.values[0]->get<std::string>();
            }
            if (res.values[1]) {
                transaction_id = res.values[1]->get<std::string>();
            }
            if (res.values[2]) {
                attempt_id = res.values[2]->get<std::string>();
            }
            if (res.values[3]) {
                staged_content = res.values[3]->get<nlohmann::json>();
            }
            if (res.values[4]) {
                atr_bucket_name = res.values[4]->get<std::string>();
            }
            if (res.values[5]) {
                std::string name = res.values[5]->get<std::string>();
                std::vector<std::string> splits;
                boost::split(splits, name, [](char c) { return c == '.'; });
                atr_scope_name = splits[0];
                atr_collection_name = splits[1];
            }
            if (res.values[6]) {
                nlohmann::json restore = *res.values[6];
                cas_pre_txn = restore["CAS"].get<std::string>();
                // only present in 6.5+
                revid_pre_txn = restore["revid"].get<std::string>();
                exptime_pre_txn = restore["exptime"].get<uint32_t>();
            }
            if (res.values[7]) {
                op = res.values[7]->get<std::string>();
            }
            if (res.values[8]) {
                nlohmann::json doc = *res.values[8];
                cas_from_doc = doc["CAS"].get<std::string>();
                // only present in 6.5+
                revid_from_doc = doc["revid"].get<std::string>();
                exptime_from_doc = doc["exptime"].get<uint32_t>();
                crc32_from_doc = doc["value_crc32c"].get<std::string>();
            }
            if (res.values[9]) {
                crc32_of_staging = res.values[9].get();
            }
            if (res.values[10]) {
                content = res.values[10].get();
            } else {
                content = nlohmann::json::object();
            }

            transaction_links links(atr_id,
                    atr_bucket_name,
                    atr_scope_name,
                    atr_collection_name,
                    transaction_id,
                    attempt_id,
                    staged_content,
                    cas_pre_txn,
                    revid_pre_txn,
                    exptime_pre_txn,
                    crc32_of_staging,
                    op);
            document_metadata md(cas_from_doc, revid_from_doc, exptime_from_doc, crc32_from_doc);
            return transaction_document(id, content, res.cas, collection, links, status, boost::make_optional(md));
        }

        // TODO: make a swap function, use that here and copy constructor
        template<typename Content>
        transaction_document& operator=(const transaction_document& other)
        {
            if (this != &other) {
                this->collection_ = other.collection_;
                this->value_ = other.value_;
                this->id_ = other.id_;
                this->links_ = other.links_;
                this->status_ = other.status_;
            }
            return *this;
        }

        collection& collection_ref()
        {
            return collection_;
        }

        template<typename Content>
        CB_NODISCARD Content content() const
        {
            return value_.get<Content>();
        }

        CB_NODISCARD const std::string& id() const
        {
            return id_;
        }

        CB_NODISCARD uint64_t cas() const
        {
            return cas_;
        }

        CB_NODISCARD transaction_links links() const
        {
            return links_;
        }

        CB_NODISCARD transaction_document_status status() const
        {
            return status_;
        }

        template<typename Content>
        void content(const Content& content)
        {
            value_ = content;
        }

        void cas(uint64_t cas)
        {
            cas_ = cas;
        }

        void status(transaction_document_status status)
        {
            status_ = status;
        }

        CB_NODISCARD const boost::optional<document_metadata>& metadata() const
        {
            return metadata_;
        }

        template<typename OStream>
        friend OStream& operator<<(OStream& os, const transaction_document document)
        {
            os << "transaction_document{id: " << document.id_ << ", cas: " << document.cas_
               << ", status: " << transaction_document_status_name(document.status_) << ", bucket: " << document.collection_.bucket_name()
               << ", coll: " << document.collection_.name() << ", links_: " << document.links_ << "}";
            return os;
        }
    };
} // namespace transactions
} // namespace couchbase
