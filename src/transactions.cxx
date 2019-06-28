#include <libcouchbase/transactions.hxx>

#include <iostream>
#include <libcouchbase/transactions/transaction_context.hxx>

couchbase::transactions::Transactions::Transactions(couchbase::Cluster &cluster, couchbase::transactions::Configuration &configuration)
{
    std::cerr << "Transactions()" << std::endl;
}

void couchbase::transactions::Transactions::close()
{
    std::cerr << "Transactions#close()" << std::endl;
}

void couchbase::transactions::Transactions::run(couchbase::transactions::Logic &logic)
{
    TransactionContext overall;
    AttemptContext ctx(overall);
    logic.run(ctx);
    if (!ctx.is_done()) {
        ctx.commit();
    }
}

