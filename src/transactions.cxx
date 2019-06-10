#include <libcouchbase/transactions.hxx>

#include <iostream>

couchbase::transactions::Transactions::Transactions(couchbase::Cluster &cluster, couchbase::transactions::Configuration &configuration)
{
    std::cerr << "Transactions()" << std::endl;
}

void couchbase::transactions::Transactions::close()
{
    std::cerr << "Transactions#close()" << std::endl;
}

couchbase::transactions::Result couchbase::transactions::Transactions::run(couchbase::transactions::Logic &logic)
{
    std::cerr << "Transactions#run()" << std::endl;
    return couchbase::transactions::Result();
}
