#include <boost/uuid/uuid.hpp>
#include <boost/uuid/uuid_generators.hpp>
#include <boost/uuid/uuid_io.hpp>

#include <couchbase/transactions/uid_generator.hxx>

namespace uuids = boost::uuids;
namespace tx = couchbase::transactions;

std::string tx::uid_generator::next()
{
    return uuids::to_string(uuids::uuid{ uuids::random_generator()() });
}
