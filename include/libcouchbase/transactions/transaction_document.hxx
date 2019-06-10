#pragma once

namespace couchbase
{
namespace transactions
{
    class TransactionDocument
    {
      private:
        std::string value_;
        std::string id_;
        uint64_t cas_;

      public:
        TransactionDocument(std::string value);

        const std::string content() const;
        const std::string id() const ;
        const uint64_t cas() const ;

        template < typename ValueParser > typename ValueParser::ValueType content_as()
        {
            return ValueParser::parse(value_);
        }
    };
} // namespace transactions
} // namespace couchbase
