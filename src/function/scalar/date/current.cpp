#include "duckdb/function/scalar/date_functions.hpp"

#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void current_time_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                  BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 0);

	auto val = Value::INTEGER(Timestamp::GetTime(Timestamp::GetCurrentTimestamp()));
	result.Initialize(TypeId::INTEGER, false);
	result.count = 1;
	result.SetValue(0, val);
}

static void current_date_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                  BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 0);

	auto val = Value::INTEGER(Timestamp::GetDate(Timestamp::GetCurrentTimestamp()));
	result.Initialize(TypeId::INTEGER, false);
	result.count = 1;
	result.SetValue(0, val);
}

static void current_timestamp_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                       BoundFunctionExpression &expr, Vector &result) {
	assert(input_count == 0);

	auto val = Value::TIMESTAMP(Timestamp::GetCurrentTimestamp());
	result.Initialize(TypeId::BIGINT, false);
	result.count = 1;
	result.SetValue(0, val);
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
