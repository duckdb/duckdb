#include "duckdb/execution/operator/helper/physical_transaction.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/valid_checker.hpp"
#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_manager.hpp"
#include "duckdb/main/config.hpp"
#include "duckdb/main/database_manager.hpp"

namespace duckdb {

SourceResultType PhysicalTransaction::GetData(ExecutionContext &context, DataChunk &chunk,
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
			auto &config = DBConfig::GetConfig(context.client);
			if (config.options.immediate_transaction_mode) {
				// if immediate transaction mode is enabled then start all transactions immediately
				auto databases = DatabaseManager::Get(client).GetDatabases(client);
				for (auto db : databases) {
					context.client.transaction.ActiveTransaction().GetTransaction(db.get());
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

	return SourceResultType::FINISHED;
}

} // namespace duckdb
