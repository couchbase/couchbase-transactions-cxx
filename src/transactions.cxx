#include <libcouchbase/transactions.hxx>

#include <iostream>
#include <libcouchbase/transactions/transaction_context.hxx>

couchbase::transactions::transactions::transactions(couchbase::cluster &cluster, couchbase::transactions::configuration &configuration)
{
    std::cerr << "transactions()" << std::endl;
}

void couchbase::transactions::transactions::close()
{
    std::cerr << "transactions#close()" << std::endl;
}

void couchbase::transactions::transactions::run(const logic &logic)
{
    transaction_context overall;
    attempt_context ctx(overall);
    logic(ctx);
    if (!ctx.is_done()) {
        ctx.commit();
    }
}
