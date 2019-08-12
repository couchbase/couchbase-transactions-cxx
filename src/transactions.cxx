#include <libcouchbase/transactions.hxx>

couchbase::transactions::transactions::transactions(couchbase::cluster &cluster,
                                                    const couchbase::transactions::configuration &configuration)
    : cluster_(cluster), config_(configuration)
{
}

void couchbase::transactions::transactions::close()
{
}

void couchbase::transactions::transactions::run(const logic &logic)
{
    transaction_context overall;
    attempt_context ctx(overall, config_);
    logic(ctx);
    if (!ctx.is_done()) {
        ctx.commit();
    }
}
