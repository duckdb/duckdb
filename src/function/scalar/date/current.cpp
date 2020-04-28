#include "duckdb/function/scalar/date_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void current_time_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() == 0);

	auto val = Value::INTEGER(Timestamp::GetTime(Timestamp::GetCurrentTimestamp()));
	result.Reference(val);
}

static void current_date_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() == 0);

	auto val = Value::INTEGER(Timestamp::GetDate(Timestamp::GetCurrentTimestamp()));
	result.Reference(val);
}

static void current_timestamp_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count() == 0);

	auto val = Value::TIMESTAMP(Timestamp::GetCurrentTimestamp());
	result.Reference(val);
}

void CurrentTimeFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("current_time", {}, SQLType::TIME, current_time_function));
}

void CurrentDateFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction(ScalarFunction("current_date", {}, SQLType::DATE, current_date_function));
}

void CurrentTimestampFun::RegisterFunction(BuiltinFunctions &set) {
	set.AddFunction({"now", "current_timestamp"}, ScalarFunction({}, SQLType::TIMESTAMP, current_timestamp_function));
}

} // namespace duckdb
