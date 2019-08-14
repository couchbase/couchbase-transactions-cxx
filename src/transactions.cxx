#include <libcouchbase/transactions.hxx>

couchbase::transactions::transactions::transactions(couchbase::cluster &cluster, const couchbase::transactions::configuration &config)
    : cluster_(cluster), config_(config), cleanup_(cluster_, config_)
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
