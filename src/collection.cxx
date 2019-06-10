#include <iostream>

#include <libcouchbase/bucket.hxx>
#include <libcouchbase/collection.hxx>

couchbase::Collection::Collection() : name_("")
{
}

const std::string &couchbase::Collection::name() const
{
    return name_;
}

void couchbase::Collection::upsert(const std::string &id, const std::string &value)
{
    std::cerr << "Collection#upsert(\"" << id << "\", \"" << value << "\")" << std::endl;
}

