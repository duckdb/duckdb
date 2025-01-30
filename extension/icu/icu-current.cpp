#include "include/icu-dateadd.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/transaction/meta_transaction.hpp"
#include "include/icu-current.hpp"
#include "include/icu-casts.hpp"

namespace duckdb {

static timestamp_t GetTransactionTimestamp(ExpressionState &state) {
	return MetaTransaction::Get(state.GetContext()).start_timestamp;
}

static void CurrentTimeFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);
	auto instant = GetTransactionTimestamp(state);
	ICUDateFunc::BindData data(state.GetContext());

	dtime_tz_t result_time(dtime_t(0), 0);
	ICUToTimeTZ::ToTimeTZ(data.calendar.get(), instant, result_time);
	auto val = Value::TIMETZ(result_time);
	result.Reference(val);
}

static void CurrentDateFunction(DataChunk &input, ExpressionState &state, Vector &result) {
	D_ASSERT(input.ColumnCount() == 0);
	auto instant = GetTransactionTimestamp(state);

	auto val = Value::DATE(ICUMakeDate::ToDate(state.GetContext(), instant));
	result.Reference(val);
}

ScalarFunction GetCurrentTimeFun() {
	ScalarFunction current_time({}, LogicalType::TIME_TZ, CurrentTimeFunction);
	current_time.stability = FunctionStability::CONSISTENT_WITHIN_QUERY;
	return current_time;
}

ScalarFunction GetCurrentDateFun() {
	ScalarFunction current_date({}, LogicalType::DATE, CurrentDateFunction);
	current_date.stability = FunctionStability::CONSISTENT_WITHIN_QUERY;
	return current_date;
}

void RegisterICUCurrentFunctions(DatabaseInstance &db) {
	//	temporal + interval
	ScalarFunctionSet current_time("get_current_time");
	current_time.AddFunction(GetCurrentTimeFun());
	ExtensionUtil::RegisterFunction(db, current_time);

	ScalarFunctionSet current_date("current_date");
	current_date.AddFunction(GetCurrentDateFun());
	ExtensionUtil::RegisterFunction(db, current_date);

	current_date.name = "today";
	ExtensionUtil::RegisterFunction(db, current_date);
}

} // namespace duckdb
