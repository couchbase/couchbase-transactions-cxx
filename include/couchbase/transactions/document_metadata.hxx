#pragma once

#include <cstdint>
#include <string>

#include <boost/optional.hpp>

namespace couchbase
{
namespace transactions
{
    /**
     * Stores some $document metadata from when the document is fetched
     */
    class document_metadata
    {
      public:
        document_metadata(boost::optional<std::string> cas, boost::optional<std::string> revid, boost::optional<std::uint32_t> exptime)
          : cas_(std::move(cas))
          , revid_(std::move(revid))
          , exptime_(exptime)
        {
        }

        [[nodiscard]] boost::optional<std::string> cas() const
        {
            return cas_;
        }

        [[nodiscard]] boost::optional<std::string> revid() const
        {
            return revid_;
        }

        [[nodiscard]] boost::optional<std::uint32_t> exptime() const
        {
            return exptime_;
        }

      private:
        const boost::optional<std::string> cas_;
        const boost::optional<std::string> revid_;
        const boost::optional<std::uint32_t> exptime_;
    };
} // namespace transactions
} // namespace couchbase
