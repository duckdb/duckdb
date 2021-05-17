#include "duckdb/execution/operator/helper/physical_transaction.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

void PhysicalTransaction::GetChunkInternal(ExecutionContext &context, DataChunk &chunk,
                                           PhysicalOperatorState *state) const {
	auto &client = context.client;

	switch (info->type) {
	case TransactionType::BEGIN_TRANSACTION: {
		if (client.transaction.IsAutoCommit()) {
			// start the active transaction
			// if autocommit is active, we have already called
			// BeginTransaction by setting autocommit to false we
			// prevent it from being closed after this query, hence
			// preserving the transaction context for the next query
			client.transaction.SetAutoCommit(false);
		} else {
			throw TransactionException("cannot start a transaction within a transaction");
		}
		break;
	}
	case TransactionType::COMMIT: {
		if (client.transaction.IsAutoCommit()) {
			throw TransactionException("cannot commit - no transaction is active");
		} else {
			// explicitly commit the current transaction
			client.transaction.Commit();
		}
		break;
	}
	case TransactionType::ROLLBACK: {
		if (client.transaction.IsAutoCommit()) {
			throw TransactionException("cannot rollback - no transaction is active");
		} else {
			// explicitly rollback the current transaction
			client.transaction.Rollback();
		}
		break;
	}
	default:
		throw NotImplementedException("Unrecognized transaction type!");
	}
	state->finished = true;
}

} // namespace duckdb
