#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include "uid_generator.hxx"

namespace uuids = boost::uuids;
namespace tx = couchbase::transactions;

std::mutex tx::uid_generator::mutex_;

std::string
tx::uid_generator::next()
{
    std::lock_guard<std::mutex> lock(mutex_);
    static auto generator = uuids::random_generator();
    return uuids::to_string(generator());
}
