#include <string>

#include "core_functions/scalar/date_functions.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/types/data_chunk.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/datetime.hpp"
#include "duckdb/common/types/validity_mask.hpp"
#include "duckdb/common/types/vector.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/execution/expression_executor_state.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"

namespace duckdb {

static void AgeFunctionStandard(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 1);
	//	Subtract argument from current_date (at midnight)
	//	Theoretically, this should be TZ-sensitive, but since we have to be able to handle
	//	plain TZ when ICU is not loaded, we implement this in UTC (like everything else)
	//	To get the PG behaviour, we overload these functions in ICU for TSTZ arguments.
	auto current_date = Timestamp::FromDatetime(
	    Timestamp::GetDate(MetaTransaction::Get(state.GetContext()).start_timestamp), dtime_t(0));

	UnaryExecutor::ExecuteWithNulls<timestamp_t, interval_t>(input.data[0], result, input.size(),
	                                                         [&](timestamp_t input, ValidityMask &mask, idx_t idx) {
		                                                         if (Timestamp::IsFinite(input)) {
			                                                         return Interval::GetAge(current_date, input);
		                                                         } else {
			                                                         mask.SetInvalid(idx);
			                                                         return interval_t();
		                                                         }
	                                                         });
}

static void AgeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 2);

	BinaryExecutor::ExecuteWithNulls<timestamp_t, timestamp_t, interval_t>(
	    input.data[0], input.data[1], result, input.size(),
	    [&](timestamp_t input1, timestamp_t input2, ValidityMask &mask, idx_t idx) {
		    if (Timestamp::IsFinite(input1) && Timestamp::IsFinite(input2)) {
			    return Interval::GetAge(input1, input2);
		    } else {
			    mask.SetInvalid(idx);
			    return interval_t();
		    }
	    });
}

ScalarFunctionSet AgeFun::GetFunctions() {
	ScalarFunctionSet age("age");
	age.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::INTERVAL, AgeFunctionStandard));
	age.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP, LogicalType::TIMESTAMP}, LogicalType::INTERVAL, AgeFunction));
	return age;
}

} // namespace duckdb
