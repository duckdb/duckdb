#include "duckdb/function/scalar/system_functions.hpp"

#include "duckdb/common/exception/transaction_exception.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/transaction/transaction_snapshot_registry.hpp"

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
	if (!context.transaction.HasActiveTransaction()) {
		throw TransactionException(
		    "duckdb_export_transaction() must be called inside an explicit transaction (BEGIN first)");
	}
	if (context.transaction.IsAutoCommit()) {
		throw TransactionException(
		    "duckdb_export_transaction() must be called inside an explicit transaction (BEGIN first)");
	}
	if (context.transaction.IsSharedParticipant()) {
		throw TransactionException("duckdb_export_transaction() can only be called from the owner of the transaction");
	}
	auto shared = context.transaction.GetSharedTransaction();
	auto &registry = context.db->GetTransactionSnapshotRegistry();
	auto id = registry.Export(shared);
	return make_uniq<ExportTransactionSnapshotData>(std::move(id));
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
