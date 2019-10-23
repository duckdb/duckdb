#include "function/scalar/date_functions.hpp"

#include "common/exception.hpp"
#include "common/types/date.hpp"
#include "common/types/timestamp.hpp"
#include "common/vector_operations/vector_operations.hpp"

using namespace std;

namespace duckdb {

static void current_time_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 0);

	auto val = Value::INTEGER(Timestamp::GetTime(Timestamp::GetCurrentTimestamp()));
	result.Reference(val);
}

static void current_date_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 0);

	auto val = Value::INTEGER(Timestamp::GetDate(Timestamp::GetCurrentTimestamp()));
	result.Reference(val);
}

static void current_timestamp_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count, BoundFunctionExpression &expr,
                   Vector &result) {
	assert(input_count == 0);

	auto val = Value::TIMESTAMP(Timestamp::GetCurrentTimestamp());
	result.Reference(val);
}

// these are created with side_effects because of its non-constant return value

void CurrentTime::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet current_time("current_time");
	current_time.AddFunction(ScalarFunction({}, SQLType::TIME, current_time_function, true));
	set.AddFunction(current_time);
}

void CurrentDate::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet current_date("current_date");
	current_date.AddFunction(ScalarFunction({}, SQLType::DATE, current_date_function, true));
	set.AddFunction(current_date);
}

void CurrentTimestamp::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet current_timestamp("current_timestamp");
	current_timestamp.AddFunction(ScalarFunction({}, SQLType::TIMESTAMP, current_timestamp_function, true));
	set.AddFunction(current_timestamp);

	ScalarFunctionSet now("now");
	now.AddFunction(ScalarFunction({}, SQLType::TIMESTAMP, current_timestamp_function, true));
	set.AddFunction(now);
}

} // namespace duckdb
