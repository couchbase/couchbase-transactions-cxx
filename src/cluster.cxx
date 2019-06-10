#include <libcouchbase/cluster.hxx>

#include <iostream>

couchbase::Cluster::Cluster(std::string cluster_address)
{
    std::cerr << "Cluster(\"" << cluster_address << "\")" << std::endl;
}

void couchbase::Cluster::authenticate(std::string user_name, std::string password)
{
    std::cerr << "Cluster#authenticate(\"" << user_name << "\", \"" << password << "\")" << std::endl;
}

couchbase::Bucket couchbase::Cluster::bucket(std::string name)
{
    std::cerr << "Cluster#bucket(\"" << name << "\")" << std::endl;
    return Bucket();
}

void couchbase::Cluster::shutdown()
{
    std::cerr << "Cluster#shutdown()" << std::endl;
}
