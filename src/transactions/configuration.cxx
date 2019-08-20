#include <couchbase/transactions/configuration.hxx>

namespace tx = couchbase::transactions;

enum tx::durability_level tx::configuration::durability_level() const
{
    return NONE;
}

void tx::configuration::durability_level(enum durability_level level)
{
    level_ = level;
}

int tx::configuration::cleanup_window() const
{
    return cleanup_window_;
}

void tx::configuration::cleanup_window(int ms)
{
    cleanup_window_ = ms;
}
