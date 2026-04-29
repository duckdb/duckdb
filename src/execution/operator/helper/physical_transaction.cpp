#include "duckdb/execution/operator/helper/physical_transaction.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/settings.hpp"

namespace duckdb {

SourceResultType PhysicalTransaction::GetDataInternal(ExecutionContext &context, DataChunk &chunk,
                                                      OperatorSourceInput &input) const {
	auto &client = context.client;

	auto type = info->type;
	if (type == TransactionType::COMMIT && ValidChecker::IsInvalidated(client.ActiveTransaction())) {
		// transaction is invalidated - turn COMMIT into ROLLBACK
		type = TransactionType::ROLLBACK;
	}
	switch (type) {
	case TransactionType::BEGIN_TRANSACTION: {
		if (client.transaction.IsAutoCommit()) {
			// start the active transaction
			// if autocommit is active, we have already called
			// BeginTransaction by setting autocommit to false we
			// prevent it from being closed after this query, hence
			// preserving the transaction context for the next query
			client.transaction.SetAutoCommit(false);
			if (info->modifier == TransactionModifierType::TRANSACTION_READ_ONLY) {
				client.transaction.SetReadOnly();
			}
			client.transaction.SetInvalidationPolicy(info->invalidation_policy);
			client.transaction.SetAutoRollback(info->auto_rollback);
			if (Settings::Get<ImmediateTransactionModeSetting>(context.client)) {
				// if immediate transaction mode is enabled then start all transactions immediately
				auto databases = DatabaseManager::Get(client).GetDatabases(client);
				for (auto &db : databases) {
					context.client.transaction.ActiveTransaction().GetTransaction(*db);
				}
			}
		} else {
			throw TransactionException("cannot start a transaction within a transaction");
		}
		break;
	}
	case TransactionType::COMMIT: {
		if (client.transaction.IsAutoCommit()) {
			throw TransactionException("cannot commit - no transaction is active");
		} else {
			// Release the per-DuckTransaction statement guards before finalizing: COMMIT may
			// destroy the underlying DuckTransaction (and therefore its statement_lock mutex),
			// after which the unique_locks held in active_query would unlock freed memory at
			// end-of-query. EndQueryInternal already does this for autocommit by resetting
			// active_query before transaction.Commit(); the explicit-COMMIT path needs the
			// same ordering.
			client.ReleaseSharedStatementGuards();
			// explicitly commit the current transaction
			client.transaction.Commit();
			// Suppress further interrupts for the remainder of this query.
			// A committed transaction is irreversible. If a concurrent Interrupt()
			// arrives after the physical commit, it must be silently discarded —
			// otherwise the caller sees a failed COMMIT even though the data
			// is already durably committed.
			client.SuppressInterrupts();
		}
		break;
	}
	case TransactionType::JOIN_TRANSACTION: {
		client.transaction.JoinTransaction(info->transaction_id);
		break;
	}
	case TransactionType::ROLLBACK: {
		if (client.transaction.IsAutoCommit()) {
			throw TransactionException("cannot rollback - no transaction is active");
		} else {
			// Same ordering requirement as COMMIT: release guards before finalizing so the
			// unique_locks don't outlive the DuckTransactions they reference.
			client.ReleaseSharedStatementGuards();
			// Explicitly rollback the current transaction
			// If it is because of an invalidated transaction, we need to rollback with an error
			auto &valid_checker = ValidChecker::Get(client.transaction.ActiveTransaction());
			if (valid_checker.IsInvalidated()) {
				ErrorData error(ExceptionType::TRANSACTION, valid_checker.InvalidatedMessage());
				client.transaction.Rollback(error);
			} else {
				client.transaction.Rollback(nullptr);
			}
		}
		break;
	}
	default:
		throw NotImplementedException("Unrecognized transaction type!");
	}

	return SourceResultType::FINISHED;
}

} // namespace duckdb
