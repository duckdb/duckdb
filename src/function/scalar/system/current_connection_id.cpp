#include "duckdb/function/scalar/system_functions.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/main/client_data.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

#include "utf8proc.hpp"

namespace duckdb {

namespace {

struct CurrentConnectionIdData : FunctionData {
	explicit CurrentConnectionIdData(Value connection_id_p) : connection_id(std::move(connection_id_p)) {
	}
	Value connection_id;

	unique_ptr<FunctionData> Copy() const override {
		return make_uniq<CurrentConnectionIdData>(connection_id);
	}
	bool Equals(const FunctionData &other_p) const override {
		return connection_id == other_p.Cast<CurrentConnectionIdData>().connection_id;
	}
};

unique_ptr<FunctionData> CurrentConnectionIdBind(BindScalarFunctionInput &input) {
	auto &context = input.GetClientContext();
	return make_uniq<CurrentConnectionIdData>(Value::UBIGINT(context.GetConnectionId()));
}

void CurrentConnectionIdFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
	const auto &info = func_expr.bind_info->Cast<CurrentConnectionIdData>();
	result.Reference(info.connection_id);
}

} // namespace

ScalarFunction CurrentConnectionId::GetFunction() {
	return ScalarFunction({}, LogicalType::UBIGINT, CurrentConnectionIdFunction, CurrentConnectionIdBind, nullptr,
	                      nullptr, LogicalType(LogicalTypeId::INVALID), FunctionStability::VOLATILE);
}

} // namespace duckdb
