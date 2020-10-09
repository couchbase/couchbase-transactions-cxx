#include <couchbase/transactions/active_transaction_record.hxx>

#include <libcouchbase/couchbase.h>

namespace couchbase
{
namespace transactions
{
    boost::optional<active_transaction_record> active_transaction_record::get_atr(std::shared_ptr<collection> collection,
                                                                                  const std::string& atr_id)
    {
        result res = collection->lookup_in(atr_id,
                                           {
                                             lookup_in_spec::get(ATR_FIELD_ATTEMPTS).xattr(),
                                           });
        if (res.rc == LCB_ERR_DOCUMENT_NOT_FOUND) {
            return {};
        } else if (res.rc == LCB_SUCCESS) {
            nlohmann::json attempts = *res.values[0];
            return map_to_atr(collection, atr_id, res, attempts);
        } else {
            throw client_error(res);
        }
    }

} // namespace transactions
} // namespace couchbase
