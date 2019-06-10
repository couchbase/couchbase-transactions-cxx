#include <iostream>

#include <libcouchbase/bucket.hxx>

couchbase::Collection couchbase::Bucket::default_collection() {
    std::cerr << "Bucket#default_collection()" << std::endl;
    return Collection();
}
