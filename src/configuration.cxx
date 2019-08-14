#include <libcouchbase/transactions/configuration.hxx>

enum couchbase::transactions::durability_level couchbase::transactions::configuration::durability_level() const
{
    return NONE;
}

void couchbase::transactions::configuration::durability_level(enum durability_level level)
{
    level_ = level;
}

int couchbase::transactions::configuration::cleanup_window() const
{
    return cleanup_window_;
}

void couchbase::transactions::configuration::cleanup_window(int ms)
{
    cleanup_window_ = ms;
}
