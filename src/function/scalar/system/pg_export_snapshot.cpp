#include "duckdb/function/scalar/system_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/transaction_context.hpp"

namespace duckdb {

namespace {

void PgExportSnapshotFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &context = state.GetContext();
	auto snapshot_id = context.transaction.ExportSnapshot();
	result.Reference(Value(snapshot_id), count_t(args.size()));
}

} // namespace

ScalarFunction PgExportSnapshot::GetFunction() {
	return ScalarFunction({}, LogicalType::VARCHAR, PgExportSnapshotFunction, nullptr, nullptr, nullptr,
	                      LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE);
}

} // namespace duckdb
