#include <libcouchbase/transactions.hxx>

couchbase::transactions::transactions::transactions(couchbase::cluster &cluster, couchbase::transactions::configuration &configuration)
{
}

void couchbase::transactions::transactions::close()
{
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
