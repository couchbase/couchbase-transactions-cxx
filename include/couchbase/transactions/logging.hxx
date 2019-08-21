#ifndef TRANSACTIONS_CXX_LOGGING_HXX
#define TRANSACTIONS_CXX_LOGGING_HXX

#include <boost/log/trivial.hpp>
#include <boost/log/sources/record_ostream.hpp>

#define LOG(logger, lvl) BOOST_LOG_STREAM_WITH_PARAMS(logger, (::boost::log::keywords::severity = ::boost::log::trivial::lvl))

#endif // TRANSACTIONS_CXX_LOGGING_HXX
