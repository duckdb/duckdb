#include "duckdb/function/scalar/date_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void current_time_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count == 0);

	auto val = Value::INTEGER(Timestamp::GetTime(Timestamp::GetCurrentTimestamp()));
	result.Reference(val);
}

static void current_date_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count == 0);

	auto val = Value::INTEGER(Timestamp::GetDate(Timestamp::GetCurrentTimestamp()));
	result.Reference(val);
}

static void current_timestamp_function(DataChunk &input, ExpressionState &state, Vector &result) {
	assert(input.column_count == 0);

	auto val = Value::TIMESTAMP(Timestamp::GetCurrentTimestamp());
	result.Reference(val);
}

void CurrentTimeFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet current_time("current_time");
	current_time.AddFunction(ScalarFunction({}, SQLType::TIME, current_time_function));
	set.AddFunction(current_time);
}

void CurrentDateFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet current_date("current_date");
	current_date.AddFunction(ScalarFunction({}, SQLType::DATE, current_date_function));
	set.AddFunction(current_date);
}

void CurrentTimestampFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet current_timestamp("current_timestamp");
	current_timestamp.AddFunction(ScalarFunction({}, SQLType::TIMESTAMP, current_timestamp_function));
	set.AddFunction(current_timestamp);

	ScalarFunctionSet now("now");
	now.AddFunction(ScalarFunction({}, SQLType::TIMESTAMP, current_timestamp_function));
	set.AddFunction(now);
}

} // namespace duckdb
