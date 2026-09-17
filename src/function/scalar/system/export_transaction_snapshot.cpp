#include "duckdb/function/scalar/system_functions.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/duck_transaction_manager.hpp"
#include "duckdb/transaction/shared_transaction_lock.hpp"

namespace duckdb {

namespace {

struct ExportTransactionSnapshotLocalState : FunctionLocalState {
	explicit ExportTransactionSnapshotLocalState(string transaction_id_p)
	    : transaction_id(std::move(transaction_id_p)) {
	}

	string transaction_id;
};

struct ExportTransactionSnapshotBindData : FunctionData {
	ExportTransactionSnapshotBindData() : has_database(false) {
	}
	explicit ExportTransactionSnapshotBindData(string database_p)
	    : has_database(true), database(std::move(database_p)) {
	}

	unique_ptr<FunctionData> Copy() const override {
		if (!has_database) {
			return make_uniq<ExportTransactionSnapshotBindData>();
		}
		return make_uniq<ExportTransactionSnapshotBindData>(database);
	}

	bool Equals(const FunctionData &other_p) const override {
		auto &other = other_p.Cast<ExportTransactionSnapshotBindData>();
		return has_database == other.has_database && database == other.database;
	}

	bool has_database;
	string database;
};

unique_ptr<FunctionData> ExportTransactionSnapshotBind(BindScalarFunctionInput &input) {
	if (input.HasBinder()) {
		input.GetBinder().GetStatementProperties().output_type = QueryResultOutputType::FORCE_MATERIALIZED;
	}
	auto database = input.GetConstant(0);
	if (database.IsNull()) {
		return make_uniq<ExportTransactionSnapshotBindData>();
	}
	return make_uniq<ExportTransactionSnapshotBindData>(database.GetValue<string>());
}

unique_ptr<FunctionLocalState> ExportTransactionSnapshotInit(ExpressionState &state, const BoundFunctionExpression &,
                                                             FunctionData *bind_data_p) {
	auto &context = state.GetContext();
	if (!context.transaction.HasActiveTransaction() || context.transaction.IsAutoCommit()) {
		throw TransactionException(
		    "duckdb_export_transaction_snapshot() must be called inside an explicit transaction");
	}
	auto &meta_transaction = context.transaction.ActiveTransaction();
	auto &database_manager = DatabaseManager::Get(context);
	auto &bind_data = bind_data_p->Cast<ExportTransactionSnapshotBindData>();
	optional_ptr<AttachedDatabase> database;
	if (bind_data.has_database) {
		auto named_database = database_manager.GetDatabase(context, Identifier(bind_data.database));
		if (!named_database) {
			throw TransactionException("duckdb_export_transaction_snapshot(): database %s does not exist",
			                           bind_data.database);
		}
		database = named_database.get();
	} else {
		database = meta_transaction.SharedDatabase();
		if (!database) {
			database = meta_transaction.ModifiedDatabase();
		}
		if (!database) {
			for (auto &opened_database : meta_transaction.OpenedTransactions()) {
				auto &candidate = opened_database.get();
				if (candidate.IsSystem() || candidate.IsTemporary()) {
					continue;
				}
				if (database) {
					throw TransactionException(
					    "duckdb_export_transaction_snapshot(): database is ambiguous; pass a database name");
				}
				database = &candidate;
			}
		}
	}
	if (!database) {
		auto name = database_manager.GetDefaultDatabase(context);
		auto default_database = database_manager.GetDatabase(context, name);
		if (!default_database) {
			throw TransactionException("duckdb_export_transaction_snapshot(): default database %s does not exist",
			                           name);
		}
		database = default_database.get();
	}
	if (database->IsSystem() || database->IsTemporary()) {
		throw TransactionException("duckdb_export_transaction_snapshot(): database %s cannot be shared",
		                           database->GetName());
	}
	meta_transaction.ValidateSharableTransaction(*database);
	auto &transaction = meta_transaction.GetTransaction(*database);
	if (!transaction.IsDuckTransaction()) {
		throw TransactionException("Database %s does not support transaction snapshots", database->GetName());
	}
	auto &duck_transaction = transaction.Cast<DuckTransaction>();
	bool newly_shared;
	auto shared_state = duck_transaction.GetTransactionManager().ShareTransaction(duck_transaction, newly_shared);
	if (newly_shared) {
		// The manager pre-locked the statement lock for this statement; hand it to the active query.
		try {
			meta_transaction.SetSharedTransaction(*database, shared_state);
			context.GuardSharedTransaction(shared_state->GetStatementLock(),
			                               SharedTransactionGuardMode::ADOPT_EXCLUSIVE);
		} catch (...) {
			shared_state->GetStatementLock()->UnlockExclusive();
			throw;
		}
	}
	return make_uniq<ExportTransactionSnapshotLocalState>(shared_state->GetToken());
}

void ExportTransactionSnapshotFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	auto &data = ExecuteFunctionState::GetFunctionState(state)->Cast<ExportTransactionSnapshotLocalState>();
	result.Reference(Value(data.transaction_id), count_t(input.size()));
}

} // namespace

ScalarFunction ExportTransactionSnapshotFun::GetFunction() {
	ScalarFunction function({FunctionParameter("database", LogicalType::VARCHAR, Value(LogicalTypeId::SQLNULL))},
	                        LogicalType::VARCHAR, ExportTransactionSnapshotFunction, ExportTransactionSnapshotBind,
	                        nullptr, ExportTransactionSnapshotInit);
	function.SetVolatile();
	function.SetFallible();
	function.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	function.SetRequiresOrderedExecution(true);
	return function;
}

} // namespace duckdb
