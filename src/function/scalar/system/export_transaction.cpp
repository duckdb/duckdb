#include "duckdb/function/scalar/system_functions.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/attached_database.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/main/database_manager.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/transaction/duck_transaction.hpp"
#include "duckdb/transaction/meta_transaction.hpp"

namespace duckdb {

namespace {

struct ExportTransactionSnapshotData : FunctionData {
	explicit ExportTransactionSnapshotData(string snapshot_id_p) : snapshot_id(std::move(snapshot_id_p)) {
	}
	string snapshot_id;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<ExportTransactionSnapshotData>(snapshot_id);
	}
	bool Equals(const FunctionData &other_p) const override {
		return snapshot_id == other_p.Cast<ExportTransactionSnapshotData>().snapshot_id;
	}
};

unique_ptr<FunctionData> ExportTransactionBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	if (!context.transaction.HasActiveTransaction() || context.transaction.IsAutoCommit()) {
		throw TransactionException(
		    "duckdb_export_transaction() must be called inside an explicit transaction (BEGIN first)");
	}
	auto &meta = context.transaction.ActiveTransaction();
	optional_ptr<AttachedDatabase> match = meta.ModifiedDatabase();
	if (!match) {
		// No database modified yet; fall back to the default (search-path) database.
		auto &db_manager = DatabaseManager::Get(context);
		auto default_db = db_manager.GetDefaultDatabase(context);
		auto db = db_manager.GetDatabase(context, default_db);
		if (!db) {
			throw TransactionException("duckdb_export_transaction(): could not resolve default database '%s'",
			                           default_db);
		}
		match = db.get();
	}
	// Ensure the transaction is materialized for this database. GetTransaction lazily starts one
	// if it doesn't exist - this gives the participant a DuckTransaction to attach to.
	auto &transaction = meta.GetTransaction(*match);
	if (!transaction.IsDuckTransaction()) {
		throw TransactionException("duckdb_export_transaction(): database '%s' does not support shared transactions",
		                           match->GetName());
	}
	auto snapshot_id =
	    StringUtil::Format("%llu/%s", static_cast<uint64_t>(context.GetConnectionId()), match->GetName());
	return make_uniq<ExportTransactionSnapshotData>(std::move(snapshot_id));
}

void ExportTransactionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<ExportTransactionSnapshotData>();
	result.Reference(Value(info.snapshot_id), count_t(args.size()));
}

} // namespace

ScalarFunction ExportTransactionSnapshot::GetFunction() {
	return ScalarFunction({}, LogicalType::VARCHAR, ExportTransactionFunction, ExportTransactionBind, nullptr, nullptr,
	                      LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE);
}

} // namespace duckdb
