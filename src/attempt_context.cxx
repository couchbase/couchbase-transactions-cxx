#include <iostream>

#include <libcouchbase/collection.hxx>
#include <libcouchbase/transactions/attempt_context.hxx>

couchbase::transactions::TransactionDocument couchbase::transactions::AttemptContext::get(Collection &collection, std::string id)
{
    std::cerr << "AttemptContext#get(\"" << collection.name() << "\", \"" << id << "\")" << std::endl;
    return TransactionDocument("{}");
}
couchbase::transactions::TransactionDocument
couchbase::transactions::AttemptContext::replace(couchbase::Collection &collection,
                                                 const couchbase::transactions::TransactionDocument &document, const std::string &content)
{
    std::cerr << "AttemptContext#replace(\"" << collection.name() << "\", \"" << document.id() << "\")" << std::endl;
    return TransactionDocument("{}");
}
couchbase::transactions::TransactionDocument
couchbase::transactions::AttemptContext::insert(couchbase::Collection &collection, const std::string &id, const std::string &content)
{
    std::cerr << "AttemptContext#insert(\"" << collection.name() << "\", \"" << id << "\")" << std::endl;
    return TransactionDocument("{}");
}

void couchbase::transactions::AttemptContext::remove(couchbase::Collection &collection,
                                                     const couchbase::transactions::TransactionDocument &document)
{
    std::cerr << "AttemptContext#remove(\"" << collection.name() << "\", \"" << document.id() << "\")" << std::endl;
}
