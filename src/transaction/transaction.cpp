#include "duckdb/transaction/transaction.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

Transaction::Transaction(TransactionManager &manager_p, ClientContext &context_p)
    : manager(manager_p), context(context_p.shared_from_this()), active_query(MAXIMUM_QUERY_ID) {
}

Transaction::~Transaction() {
}

bool Transaction::IsReadOnly() {
	auto ctxt = context.lock();
	if (!ctxt) {
		throw InternalException("Transaction::IsReadOnly() called after client context has been destroyed");
	}
	auto &db = manager.GetDB();
	return MetaTransaction::Get(*ctxt).ModifiedDatabase().get() != &db;
}

} // namespace duckdb
