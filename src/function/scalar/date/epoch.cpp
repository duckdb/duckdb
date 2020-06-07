#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"

using namespace std;

namespace duckdb {

static void epoch_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() == 1);

	string output_buffer;
	UnaryExecutor::Execute<int64_t, timestamp_t, true>(input.data[0], result, input.size(), [&](int64_t input) {
		auto ms_per_day = (int64_t)60 * 60 * 24 * 1000;
		auto date = Date::EpochToDate(input / 1000);
		auto time = (dtime_t)(std::abs(input) % ms_per_day);
		if (input < 0) { // for dates before 1970 time goes backwards
			time = ms_per_day - time;
			if (time > 0) {
				// date needs to go one back if time is non-zero
				date -= 1;
			}
			if (time == ms_per_day) {
				time = 0;
				date += 1;
			}
		}
		return Timestamp::FromDatetime(date, time);
	});
}

void EpochFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet epoch("epoch_ms");
	epoch.AddFunction(ScalarFunction({SQLType::BIGINT}, SQLType::TIMESTAMP, epoch_function));
	set.AddFunction(epoch);
}

} // namespace duckdb
