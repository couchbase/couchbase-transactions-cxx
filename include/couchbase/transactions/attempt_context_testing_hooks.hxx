#ifndef TRANSACTIONS_CXX_TESTING_HOOKS_HXX
#define TRANSACTIONS_CXX_TESTING_HOOKS_HXX

#include <functional>

namespace couchbase
{
namespace transactions
{
    class attempt_context;
    namespace
    {
        int noop_1(attempt_context*)
        {
            return 0;
        }

        int noop_2(attempt_context*, const std::string&)
        {
            return 0;
        }

        boost::optional<std::string> noop_3(attempt_context*)
        {
            return {};
        }

        bool noop_4(attempt_context*, const std::string&, boost::optional<const std::string>)
        {
            return false;
        }
    } // namespace

    static const std::string STAGE_ROLLBACK = "rollback";
    static const std::string STAGE_GET = "get";
    static const std::string STAGE_INSERT = "insert";
    static const std::string STAGE_REPLACE = "replace";
    static const std::string STAGE_REMOVE = "remove";
    static const std::string STAGE_BEFORE_COMMIT = "commit";
    static const std::string STAGE_ABORT_GET_ATR = "abortGetAtr";
    static const std::string STAGE_ROLLBACK_DOC = "rollbackDoc";
    static const std::string STAGE_DELETE_INSERTED = "deleteInserted";
    static const std::string STAGE_CREATE_STAGED_INSERT = "createdStagedInsert";
    static const std::string STAGE_REMOVE_DOC = "removeDoc";
    static const std::string STAGE_COMMIT_DOC = "commitDoc";

    static const std::string STAGE_ATR_COMMIT = "atrCommit";
    static const std::string STAGE_ATR_ABORT = "atrAbort";
    static const std::string STAGE_ATR_ROLLBACK_COMPLETE = "atrRollbackComplete";
    static const std::string STAGE_ATR_PENDING = "atrPending";
    static const std::string STAGE_ATR_COMPLETE = "atrComplete";

    /**
     * Hooks purely for testing purposes.  If you're an end-user looking at these for any reason, then please contact us first
     * about your use-case: we are always open to adding good ideas into the transactions library.
     */
    struct attempt_context_testing_hooks {
        std::function<int(attempt_context*)> before_atr_commit = noop_1;
        std::function<int(attempt_context*)> after_atr_commit = noop_1;
        std::function<int(attempt_context*, const std::string&)> before_doc_committed = noop_2;
        std::function<int(attempt_context*, const std::string&)> before_removing_doc_during_staged_insert = noop_2;
        std::function<int(attempt_context*, const std::string&)> before_rollback_delete_inserted = noop_2;
        std::function<int(attempt_context*, const std::string&)> after_doc_committed_before_saving_cas = noop_2;
        std::function<int(attempt_context*, const std::string&)> after_doc_committed = noop_2;
        std::function<int(attempt_context*, const std::string&)> before_staged_insert = noop_2;
        std::function<int(attempt_context*, const std::string&)> before_staged_remove = noop_2;
        std::function<int(attempt_context*, const std::string&)> before_staged_replace = noop_2;
        std::function<int(attempt_context*, const std::string&)> before_doc_removed = noop_2;
        std::function<int(attempt_context*, const std::string&)> before_doc_rolled_back = noop_2;
        std::function<int(attempt_context*, const std::string&)> after_doc_removed_pre_retry = noop_2;
        std::function<int(attempt_context*, const std::string&)> after_doc_removed_post_retry = noop_2;
        std::function<int(attempt_context*, const std::string&)> after_get_complete = noop_2;
        std::function<int(attempt_context*, const std::string&)> after_staged_replace_complete_before_cas_saved = noop_2;
        std::function<int(attempt_context*, const std::string&)> after_staged_replace_complete = noop_2;
        std::function<int(attempt_context*, const std::string&)> after_staged_remove_complete = noop_2;
        std::function<int(attempt_context*, const std::string&)> after_staged_insert_complete = noop_2;
        std::function<int(attempt_context*, const std::string&)> after_rollback_replace_or_remove = noop_2;
        std::function<int(attempt_context*, const std::string&)> after_rollback_delete_inserted = noop_2;
        std::function<int(attempt_context*, const std::string&)> before_check_atr_entry_for_blocking_doc = noop_2;
        std::function<int(attempt_context*, const std::string&)> before_doc_get = noop_2;
        std::function<int(attempt_context*, const std::string&)> before_get_doc_in_exists_during_staged_insert = noop_2;

        std::function<int(attempt_context*)> after_docs_committed = noop_1;
        std::function<int(attempt_context*)> after_docs_removed = noop_1;
        std::function<int(attempt_context*)> after_atr_pending = noop_1;
        std::function<int(attempt_context*)> before_atr_pending = noop_1;
        std::function<int(attempt_context*)> before_atr_complete = noop_1;
        std::function<int(attempt_context*)> before_atr_rolled_back = noop_1;
        std::function<int(attempt_context*)> after_atr_complete = noop_1;
        std::function<int(attempt_context*)> before_get_atr_for_abort = noop_1;
        std::function<int(attempt_context*)> before_atr_aborted = noop_1;
        std::function<int(attempt_context*)> after_atr_aborted = noop_1;
        std::function<int(attempt_context*)> after_atr_rolled_back = noop_1;

        std::function<boost::optional<std::string>(attempt_context*)> random_atr_id_for_vbucket = noop_3;

        std::function<bool(attempt_context*, const std::string&, boost::optional<const std::string>)> has_expired_client_side_hook = noop_4;
    };
} // namespace transactions
} // namespace couchbase

#endif // TRANSACTIONS_CXX_TESTING_HOOKS_HXX
