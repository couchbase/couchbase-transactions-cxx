#include <random>
#include <chrono>
#include <sstream>

#include <libcouchbase/transactions/uid_generator.hxx>

std::string couchbase::transactions::UidGenerator::next()
{
    static auto rnd = std::default_random_engine(std::chrono::system_clock::now().time_since_epoch().count());
    std::uniform_int_distribution< uint64_t > dist;

    uint64_t high = dist(rnd);
    uint64_t low = dist(rnd);

    std::stringstream ss;
    ss << std::hex << ((high >> 32u) & 0xffffffffu) << "-" << ((high >> 16u) & 0xffffu) << "-" << (high & 0xffffu) << "-"
       << ((low >> 48u) & 0xffffu) << "-" << (low & 0xffffffffffffu);

    return ss.str();
}
