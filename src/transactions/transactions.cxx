#include <couchbase/transactions.hxx>
#include <couchbase/transactions/logging.hxx>

namespace tx = couchbase::transactions;

tx::transactions::transactions(couchbase::cluster &cluster, const tx::configuration &config)
    : cluster_(cluster), config_(config), cleanup_(cluster_, config_)
{
}

void tx::transactions::close()
{
}

void tx::transactions::run(const logic &logic)
{
    transaction_context overall;
    LOG(log_, info) << "Running transaction context " << overall.id();
    attempt_context ctx(overall, config_);
    logic(ctx);
    if (!ctx.is_done()) {
        ctx.commit();
    }
}
