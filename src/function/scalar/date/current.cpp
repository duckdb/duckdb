#include "duckdb/function/scalar/date_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/transaction/transaction.hpp"

using namespace std;

namespace duckdb {

struct CurrentBindData : public FunctionData {
	ClientContext &context;

	CurrentBindData(ClientContext &context) : context(context) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<CurrentBindData>(context);
	}
};

static timestamp_t get_transaction_ts(ExpressionState &state) {
	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (CurrentBindData &)*func_expr.bind_info;
	return info.context.ActiveTransaction().start_timestamp;
}

static void current_time_function(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);

	auto val = Value::TIME(Timestamp::GetTime(get_transaction_ts(state)));
	result.Reference(val);
}

static void current_date_function(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);

	auto val = Value::DATE(Timestamp::GetDate(get_transaction_ts(state)));
	result.Reference(val);
}

static void current_timestamp_function(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);

	auto val = Value::TIMESTAMP(get_transaction_ts(state));
	result.Reference(val);
}

unique_ptr<FunctionData> current_bind(ClientContext &context, ScalarFunction &bound_function,
                                      vector<unique_ptr<Expression>> &arguments) {
	return make_unique<CurrentBindData>(context);
}

void CurrentTimeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("current_time", {}, LogicalType::TIME, current_time_function, false, current_bind));
}

void CurrentDateFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("current_date", {}, LogicalType::DATE, current_date_function, false, current_bind));
}

void CurrentTimestampFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"now", "current_timestamp"},
	                ScalarFunction({}, LogicalType::TIMESTAMP, current_timestamp_function, false, current_bind));
}

} // namespace duckdb
