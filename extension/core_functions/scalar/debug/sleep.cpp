#include <stdint.h>

#include "core_functions/scalar/debug_functions.hpp"
#include "duckdb/common/vector_operations/generic_executor.hpp"
#include "duckdb/common/thread.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/vector/flat_vector.hpp"
#include "duckdb/function/function.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {
struct ExpressionState;

struct NullResultType {
	using STRUCT_STATE = PrimitiveTypeState;

	static void AssignResult(Vector &result, idx_t i, NullResultType) {
		FlatVector::SetNull(result, i, true);
	}
};

static void SleepFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	input.Flatten();
	GenericExecutor::ExecuteUnary<PrimitiveType<int64_t>, NullResultType>(input.data[0], result, input.size(),
	                                                                      [](PrimitiveType<int64_t> input) {
		                                                                      // Sleep for the specified number of
		                                                                      // milliseconds (clamp negative values to
		                                                                      // 0)
		                                                                      int64_t sleep_ms = input.val;
		                                                                      if (sleep_ms < 0) {
			                                                                      sleep_ms = 0;
		                                                                      }
		                                                                      ThreadUtil::SleepMs(sleep_ms);
		                                                                      return NullResultType();
	                                                                      });
}

ScalarFunction SleepMsFun::GetFunction() {
	auto sleep_fun = ScalarFunction({LogicalType::BIGINT}, LogicalType::SQLNULL, SleepFunction, nullptr);
	sleep_fun.SetVolatile();
	sleep_fun.SetNullHandling(FunctionNullHandling::SPECIAL_HANDLING);
	return sleep_fun;
}

} // namespace duckdb
