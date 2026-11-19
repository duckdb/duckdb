#include "duckdb/function/scalar/system_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/transaction_context.hpp"

namespace duckdb {

namespace {

void ShareTransactionFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto context = state.GetContext().TryGetClientContext();
	if (!context) {
		throw InvalidInputException("share_transaction requires a client connection");
	}
	auto share_id = context->transaction.ShareTransaction();
	result.Reference(Value(share_id), count_t(args.size()));
}

} // namespace

ScalarFunction ShareTransaction::GetFunction() {
	return ScalarFunction({}, LogicalType::VARCHAR, ShareTransactionFunction, nullptr, nullptr, nullptr,
	                      LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE);
}

} // namespace duckdb
