#include "core_functions/scalar/debug_functions.hpp"

#include "duckdb/planner/expression/bound_function_expression.hpp"
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

ScalarFunction SleepFun::GetFunction() {
	auto sleep_fun = ScalarFunction("sleep_ms", {LogicalType::BIGINT}, LogicalType::SQLNULL, SleepFunction, nullptr);
	sleep_fun.stability = FunctionStability::VOLATILE;
	return sleep_fun;
}

} // namespace duckdb
