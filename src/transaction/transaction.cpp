#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

Transaction::Transaction(TransactionManager &manager_p)
    : manager(manager_p), active_query(MAXIMUM_QUERY_ID), is_read_only(true) {
}

Transaction::~Transaction() {
}

bool Transaction::IsReadOnly() {
	return is_read_only;
}

void Transaction::SetReadWrite() {
	D_ASSERT(is_read_only);
	is_read_only = false;
}

void Transaction::SetModifications(DatabaseModificationType type) {
}

} // namespace duckdb
