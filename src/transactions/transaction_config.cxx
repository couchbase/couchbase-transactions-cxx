#include "attempt_context_testing_hooks.hxx"
#include "cleanup_testing_hooks.hxx"
#include <couchbase/transactions/transaction_config.hxx>

namespace couchbase
{
namespace transactions
{

    transaction_config::transaction_config()
      : level_(durability_level::MAJORITY)
      , cleanup_window_(std::chrono::seconds(120))
      , expiration_time_(std::chrono::seconds(15))
      , cleanup_lost_attempts_(true)
      , cleanup_client_attempts_(true)
      , attempt_context_hooks_(new attempt_context_testing_hooks())
      , cleanup_hooks_(new cleanup_testing_hooks())
    {
    }

    transaction_config::~transaction_config() = default;

    transaction_config::transaction_config(const transaction_config& config)
      : level_(config.durability_level())
      , cleanup_window_(config.cleanup_window())
      , expiration_time_(config.expiration_time())
      , cleanup_lost_attempts_(config.cleanup_lost_attempts())
      , cleanup_client_attempts_(config.cleanup_client_attempts())
      , attempt_context_hooks_(new attempt_context_testing_hooks(config.attempt_context_hooks()))
      , cleanup_hooks_(new cleanup_testing_hooks(config.cleanup_hooks()))

    {
    }

    transaction_config& transaction_config::operator=(const transaction_config& c)
    {
        level_ = c.durability_level();
        cleanup_window_ = c.cleanup_window();
        expiration_time_ = c.expiration_time();
        cleanup_lost_attempts_ = c.cleanup_lost_attempts();
        cleanup_client_attempts_ = c.cleanup_client_attempts();
        attempt_context_hooks_.reset(new attempt_context_testing_hooks(c.attempt_context_hooks()));
        cleanup_hooks_.reset(new cleanup_testing_hooks(c.cleanup_hooks()));
        return *this;
    }

    void transaction_config::test_factories(attempt_context_testing_hooks& hooks, cleanup_testing_hooks& cleanup_hooks)
    {
        attempt_context_hooks_.reset(new attempt_context_testing_hooks(hooks));
        cleanup_hooks_.reset(new cleanup_testing_hooks(cleanup_hooks));
    }

} // namespace transactions
} // namespace couchbase
