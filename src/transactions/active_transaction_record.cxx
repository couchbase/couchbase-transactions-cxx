#include <couchbase/transactions/active_transaction_record.hxx>

#include <libcouchbase/couchbase.h>

namespace couchbase
{
namespace transactions
{
    boost::optional<active_transaction_record> active_transaction_record::get_atr(collection* collection,
                                                                                  const std::string& atr_id,
                                                                                  const transaction_config& config)
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
            throw client_error("got error while getting ATR " + atr_id + ": " + res.strerror());
        }
    }

} // namespace transactions
} // namespace couchbase
