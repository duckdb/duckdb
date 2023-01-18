#include "duckdb/transaction/transaction.hpp"

namespace duckdb {

Transaction::Transaction(TransactionManager &manager_p, ClientContext &context_p)
    : manager(manager_p), context(context_p.shared_from_this()), active_query(MAXIMUM_QUERY_ID) {
}

Transaction::~Transaction() {
}

} // namespace duckdb
