#include <libcouchbase/transactions/configuration.hxx>

enum couchbase::transactions::durability_level couchbase::transactions::configuration::durability_level() const
{
    return NONE;
}
void couchbase::transactions::configuration::durability_level(enum durability_level level)
{
    level_ = level;
}
