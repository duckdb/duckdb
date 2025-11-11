#include "core_functions/scalar/debug_functions.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include <thread>
#include <chrono>

namespace duckdb {

static void SleepFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	UnifiedVectorFormat vdata;
	input.data[0].ToUnifiedFormat(input.size(), vdata);

	auto milliseconds = UnifiedVectorFormat::GetData<int64_t>(vdata);
	
	for (idx_t i = 0; i < input.size(); i++) {
		auto idx = vdata.sel->get_index(i);
		if (!vdata.validity.RowIsValid(idx)) {
			FlatVector::SetNull(result, i, true);
			continue;
		}
		// Sleep for the specified number of milliseconds (clamp negative values to 0)
		int64_t sleep_ms = milliseconds[idx];
		if (sleep_ms < 0) {
			sleep_ms = 0;
		}
		std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
		FlatVector::SetNull(result, i, true);
	}
}

static unique_ptr<FunctionData> SleepBind(ClientContext &context, ScalarFunction &bound_function,
                                          vector<unique_ptr<Expression>> &arguments) {
	// Validate that we have exactly one argument
	D_ASSERT(arguments.size() == 1);
	
	// Return type is always SQLNULL
	bound_function.return_type = LogicalType::SQLNULL;
	
	return nullptr;
}

static unique_ptr<Expression> BindSleepFunctionExpression(FunctionBindExpressionInput &input) {
	if (input.children[0]->IsFoldable()) {
		Value value = ExpressionExecutor::EvaluateScalar(input.context, *input.children[0]);
		if (!value.IsNull()) {
			int64_t sleep_ms = value.GetValue<int64_t>();
			if (sleep_ms < 0) {
				sleep_ms = 0;
			}
			std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
		}
		return make_uniq<BoundConstantExpression>(Value(LogicalType::SQLNULL));
	}
	// If not constant, return nullptr to use normal function execution
	return nullptr;
}

ScalarFunction SleepFun::GetFunction() {
	auto sleep_fun = ScalarFunction("sleep",
	                                {LogicalType::BIGINT},
	                                LogicalType::SQLNULL,
	                                SleepFunction,
	                                SleepBind);
	sleep_fun.stability = FunctionStability::VOLATILE;
	sleep_fun.bind_expression = BindSleepFunctionExpression;
	return sleep_fun;
}

} 

