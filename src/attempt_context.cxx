#include <iostream>

#include <libcouchbase/collection.hxx>
#include <libcouchbase/transactions/attempt_context.hxx>
#include <libcouchbase/transactions/uid_generator.hxx>
#include <libcouchbase/transactions/attempt_state.hxx>
#include <libcouchbase/transactions/transaction_fields.hxx>
#include <json11.hpp>

#include "atr_ids.hxx"

couchbase::transactions::AttemptContext::AttemptContext(couchbase::transactions::TransactionContext &transaction_ctx)
    : transaction_ctx_(transaction_ctx), state_(AttemptState::NOT_STARTED), atr_id_(""), atr_collection_(nullptr), is_done_(false)
{
    id_ = UidGenerator::next();
}

void couchbase::transactions::AttemptContext::init_atr_if_needed(couchbase::Collection *collection, const std::string &id)
{
    if (atr_id_.empty()) {
        int vbucket_id = AtrIds::vbucket_for_key(id);
        atr_id_ = AtrIds::atr_id_for_vbucket(vbucket_id);
        atr_collection_ = collection;
        state_ = AttemptState::PENDING;
        std::cout << "First mutated doc in transaction is \"" << id << "\" on vbucket " << vbucket_id << ", so using atr \"" << atr_id_
                  << "\"" << std::endl;
    }
}

bool couchbase::transactions::AttemptContext::is_done()
{
    return is_done_;
}

couchbase::transactions::TransactionDocument couchbase::transactions::AttemptContext::get(Collection *collection, const std::string &id)
{
    StagedMutation *mutation = staged_mutations_.find_replace(collection, id);
    if (mutation == nullptr) {
        mutation = staged_mutations_.find_insert(collection, id);
    }
    if (mutation) {
        return TransactionDocument(*collection, id, mutation->content(), 0, TransactionDocumentStatus::OWN_WRITE,
                                   TransactionLinks(mutation->doc().links()));
    }
    mutation = staged_mutations_.find_remove(collection, id);
    if (mutation) {
        throw std::runtime_error(std::string("not found"));
    }

    const Result &res = collection->lookup_in(id, { LookupInSpec::get(ATR_ID).xattr(), LookupInSpec::get(STAGED_VERSION).xattr(),
                                                    LookupInSpec::get(STAGED_DATA).xattr(), LookupInSpec::get(ATR_BUCKET_NAME).xattr(),
                                                    LookupInSpec::get(ATR_SCOPE_NAME).xattr(), LookupInSpec::get(ATR_COLL_NAME).xattr(),
                                                    LookupInSpec::fulldoc_get() });
    if (res.rc == LCB_SUCCESS) {
        std::string atr_id = res.values[0];
        std::string staged_version = res.values[1];
        std::string staged_data = res.values[2];
        std::string atr_bucket_name = res.values[3];
        std::string atr_scope_name = res.values[4];
        std::string atr_coll_name = res.values[5];
        std::string content = res.values[6];

        TransactionDocument doc(*collection, id, content, res.cas, TransactionDocumentStatus::NORMAL,
                                TransactionLinks(atr_id, atr_bucket_name, atr_scope_name, atr_coll_name, content, id));

        if (doc.links().is_document_in_transaction()) {
            const Result &atr_res = collection->lookup_in(doc.links().atr_id(), { LookupInSpec::get(ATR_FIELD_ATTEMPTS).xattr() });
            if (!atr_res.values[0].empty()) {
                std::string err;
                const json11::Json &atr = json11::Json::parse(atr_res.values[0], err);
                const json11::Json &entry = atr[id_];
                if (entry.is_null()) {
                    // Don't know if txn was committed or rolled back.  Should not happen as ATR record should stick around long enough.
                    doc.status(TransactionDocumentStatus::AMBIGUOUS);
                    if (doc.content().empty()) {
                        throw std::runtime_error(std::string("not found"));
                    }
                } else {
                    if (doc.links().staged_version().empty()) {
                        if (entry["status"] == "COMMITTED") {
                            if (doc.links().is_document_being_removed()) {
                                throw std::runtime_error(std::string("not found"));
                            } else {
                                doc.content(doc.links().staged_content());
                                doc.status(TransactionDocumentStatus::IN_TXN_COMMITTED);
                            }
                        } else {
                            doc.status(TransactionDocumentStatus::IN_TXN_OTHER);
                            if (doc.content().empty()) {
                                throw std::runtime_error(std::string("not found"));
                            }
                        }
                    } else {
                        doc.content(doc.links().staged_content());
                        doc.status(TransactionDocumentStatus::OWN_WRITE);
                    }
                }
            }
        }
        return doc;
    }
    throw std::runtime_error(std::string("failed to get the document: ") + lcb_strerror_short(res.rc));
}

couchbase::transactions::TransactionDocument
couchbase::transactions::AttemptContext::replace(couchbase::Collection *collection,
                                                 const couchbase::transactions::TransactionDocument &document, const std::string &content)
{
    init_atr_if_needed(collection, document.id());

    if (staged_mutations_.empty()) {
        std::string prefix(ATR_FIELD_ATTEMPTS + "." + id_ + ".");
        collection->mutate_in(
            document.id(),
            {
                MutateInSpec::insert(prefix + ATR_FIELD_STATUS, attempt_state_name(AttemptState::PENDING)).xattr().create_path(),
                MutateInSpec::insert(prefix + ATR_FIELD_START_TIMESTAMP, "${Mutation.CAS}").xattr().expand_macro(),
                MutateInSpec::insert(prefix + ATR_FIELD_EXPIRES_AFTER_MSECS, "15").xattr(),
                MutateInSpec::fulldoc_upsert("{}"),
            });
    }

    const Result &res = collection->mutate_in(document.id(), {
                                                                 MutateInSpec::upsert(STAGED_VERSION, id_).xattr().create_path(),
                                                                 MutateInSpec::upsert(ATR_ID, atr_id_).xattr(),
                                                                 MutateInSpec::upsert(STAGED_DATA, content).xattr(),
                                                                 MutateInSpec::upsert(ATR_BUCKET_NAME, collection->bucket_name()).xattr(),
                                                                 MutateInSpec::upsert(ATR_SCOPE_NAME, collection->scope()).xattr(),
                                                                 MutateInSpec::upsert(ATR_COLL_NAME, collection->name()).xattr(),
                                                             });

    if (res.rc == LCB_SUCCESS) {
        TransactionDocument out(
            *collection, document.id(), document.content(), res.cas, TransactionDocumentStatus::NORMAL,
            TransactionLinks(atr_id_, collection->bucket_name(), collection->scope(), collection->name(), content, id_));
        staged_mutations_.add(StagedMutation(out, document.content(), StagedMutationType::REPLACE));
        return out;
    }
    throw std::runtime_error(std::string("failed to replace the document: ") + lcb_strerror_short(res.rc));
}

couchbase::transactions::TransactionDocument
couchbase::transactions::AttemptContext::insert(couchbase::Collection *collection, const std::string &id, const std::string &content)
{
    init_atr_if_needed(collection, id);

    if (staged_mutations_.empty()) {
        std::string prefix(ATR_FIELD_ATTEMPTS + "." + id_ + ".");
        collection->mutate_in(
            id, {
                    MutateInSpec::insert(prefix + ATR_FIELD_STATUS, attempt_state_name(AttemptState::PENDING)).xattr().create_path(),
                    MutateInSpec::insert(prefix + ATR_FIELD_START_TIMESTAMP, "${Mutation.CAS}").xattr().expand_macro(),
                    MutateInSpec::insert(prefix + ATR_FIELD_EXPIRES_AFTER_MSECS, "15").xattr(),
                    MutateInSpec::fulldoc_upsert("{}"),
                });
    }

    const Result &res = collection->mutate_in(id, {
                                                      MutateInSpec::upsert(STAGED_VERSION, id_).xattr().create_path(),
                                                      MutateInSpec::insert(ATR_ID, atr_id_).xattr(),
                                                      MutateInSpec::insert(STAGED_DATA, content).xattr(),
                                                      MutateInSpec::insert(ATR_BUCKET_NAME, collection->bucket_name()).xattr(),
                                                      MutateInSpec::insert(ATR_SCOPE_NAME, collection->scope()).xattr(),
                                                      MutateInSpec::insert(ATR_COLL_NAME, collection->name()).xattr(),
                                                      MutateInSpec::fulldoc_insert("{}"),
                                                  });
    if (res.rc == LCB_SUCCESS) {
        TransactionDocument out(
            *collection, id, content, res.cas, TransactionDocumentStatus::NORMAL,
            TransactionLinks(atr_id_, collection->bucket_name(), collection->scope(), collection->name(), content, id_));
        staged_mutations_.add(StagedMutation(out, content, StagedMutationType::INSERT));
        return out;
    }
    throw std::runtime_error(std::string("failed to insert the document: ") + lcb_strerror_short(res.rc));
}

void couchbase::transactions::AttemptContext::remove(couchbase::Collection *collection,
                                                     couchbase::transactions::TransactionDocument &document)
{
    init_atr_if_needed(collection, document.id());

    if (staged_mutations_.empty()) {
        std::string prefix(ATR_FIELD_ATTEMPTS + "." + id_ + ".");
        collection->mutate_in(
            document.id(),
            {
                MutateInSpec::insert(prefix + ATR_FIELD_STATUS, attempt_state_name(AttemptState::PENDING)).xattr().create_path(),
                MutateInSpec::insert(prefix + ATR_FIELD_START_TIMESTAMP, "${Mutation.CAS}").xattr().expand_macro(),
                MutateInSpec::insert(prefix + ATR_FIELD_EXPIRES_AFTER_MSECS, "15").xattr(),
                MutateInSpec::fulldoc_upsert("{}"),
            });
    }

    const Result &res = collection->mutate_in(document.id(), {
                                                                 MutateInSpec::upsert(STAGED_VERSION, id_).xattr().create_path(),
                                                                 MutateInSpec::upsert(ATR_ID, atr_id_).xattr(),
                                                                 MutateInSpec::upsert(STAGED_DATA, STAGED_DATA_REMOVED_VALUE).xattr(),
                                                                 MutateInSpec::upsert(ATR_BUCKET_NAME, collection->bucket_name()).xattr(),
                                                                 MutateInSpec::upsert(ATR_SCOPE_NAME, collection->scope()).xattr(),
                                                                 MutateInSpec::upsert(ATR_COLL_NAME, collection->name()).xattr(),
                                                             });
    if (res.rc == LCB_SUCCESS) {
        document.cas(res.cas);
        staged_mutations_.add(StagedMutation(document, "", StagedMutationType::REMOVE));
        return;
    }
    throw std::runtime_error(std::string("failed to remove the document: ") + lcb_strerror_short(res.rc));
}

void couchbase::transactions::AttemptContext::commit()
{
    std::string prefix(ATR_FIELD_ATTEMPTS + "." + id_ + ".");
    std::vector< MutateInSpec > specs({
        MutateInSpec::upsert(prefix + ATR_FIELD_STATUS, attempt_state_name(AttemptState::COMMITTED)).xattr(),
        MutateInSpec::upsert(prefix + ATR_FIELD_START_COMMIT, "${Mutation.CAS}").xattr().expand_macro(),
    });
    staged_mutations_.extract_to(prefix, specs);
    atr_collection_->mutate_in(atr_id_, specs);
    is_done_ = true;
    state_ = AttemptState::COMMITTED;
}
