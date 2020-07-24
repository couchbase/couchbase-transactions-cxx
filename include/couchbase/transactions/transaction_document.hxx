#pragma once

#include <ostream>

#include <boost/algorithm/string/split.hpp>

#include <nlohmann/json.hpp>

#include <couchbase/client/collection.hxx>
#include <couchbase/transactions/document_metadata.hxx>
#include <couchbase/transactions/transaction_document_status.hxx>
#include <couchbase/transactions/transaction_links.hxx>
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
        template<typename Content>
        transaction_document(const transaction_document& doc) :
            collection_(doc.collection_),
            value_(doc.value_),
            id_(doc.id_),
            links_(doc.links_),
            status_(doc.status_) {}

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
                                    document.links().op());

            return transaction_document(
              document.id(), content, document.cas(), document.collection_ref(), links, status, document.metadata());
        }

        static transaction_document create_from(collection& collection, std::string id, result res, transaction_document_status status)
        {
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

            // read from $document
            boost::optional<std::string> cas_from_doc;
            boost::optional<std::string> revid_from_doc;
            boost::optional<uint32_t> exptime_from_doc;

            boost::optional<std::string> op;

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
                staged_content = res.values[3]->get<std::string>();
            }
            if (res.values[4]) {
                atr_bucket_name = res.values[3]->get<std::string>();
            }
            if (res.values[5]) {
                std::string name = res.values[3]->get<std::string>();
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
            }
            nlohmann::json content = res.values[9].get();

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
                                    op);
            document_metadata md(cas_from_doc, revid_from_doc, exptime_from_doc);
            return transaction_document(std::move(id), content, res.cas, collection, links, status, boost::make_optional(md));
        }

        // TODO: make a swap function, use that here and copy constructor
        template<typename Content>
        transaction_document& operator=(const transaction_document& other) {
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
        [[nodiscard]] Content content() const
        {
            return value_.get<Content>();
        }

        [[nodiscard]] const std::string& id() const
        {
            return id_;
        }

        [[nodiscard]] uint64_t cas() const
        {
            return cas_;
        }

        [[nodiscard]] transaction_links links() const
        {
            return links_;
        }

        [[nodiscard]] transaction_document_status status() const
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

        [[nodiscard]] const boost::optional<document_metadata>& metadata() const
        {
            return metadata_;
        }

        friend std::ostream& operator<<(std::ostream& os, const transaction_document& document);
    };

    std::ostream& operator<<(std::ostream& os, const transaction_document& document);
} // namespace transactions
} // namespace couchbase
