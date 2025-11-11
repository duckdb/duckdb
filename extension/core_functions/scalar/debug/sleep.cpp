#include "core_functions/scalar/debug_functions.hpp"

#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include <thread>
#include <chrono>

namespace duckdb {

namespace {

struct NullResultType {
	using STRUCT_STATE = PrimitiveTypeState;

	static void AssignResult(Vector &result, idx_t i, NullResultType) {
		FlatVector::SetNull(result, i, true);
	}
};

} // namespace

static void SleepFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	input.Flatten();
	GenericExecutor::ExecuteUnary<PrimitiveType<int64_t>, NullResultType>(
	    input.data[0], result, input.size(), [](PrimitiveType<int64_t> input) {
		    // Sleep for the specified number of milliseconds (clamp negative values to 0)
		    int64_t sleep_ms = input.val;
		    if (sleep_ms < 0) {
			    sleep_ms = 0;
		    }
		    std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
		    return NullResultType();
	    });
}

ScalarFunction SleepMsFun::GetFunction() {
	auto sleep_fun = ScalarFunction("sleep_ms", {LogicalType::BIGINT}, LogicalType::SQLNULL, SleepFunction, nullptr);
	sleep_fun.stability = FunctionStability::VOLATILE;
	return sleep_fun;
}

} // namespace duckdb
