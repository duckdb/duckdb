#include "include/icu-dateadd.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

struct ICUCalendarAge : public ICUDateFunc {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right, icu::Calendar *calendar) {
		throw InternalException("Unimplemented type for ICUCalendarAge");
	}
};

template <>
interval_t ICUCalendarAge::Operation(timestamp_t end_date, timestamp_t start_date, icu::Calendar *calendar) {
	if (start_date > end_date) {
		auto negated = Operation<timestamp_t, timestamp_t, interval_t>(start_date, end_date, calendar);
		return {-negated.months, -negated.days, -negated.micros};
	}

	auto start_micros = ICUDateFunc::SetTime(calendar, start_date);
	auto end_micros = (uint64_t)(end_date.value % Interval::MICROS_PER_MSEC);

	// Borrow 1ms from end_date if we wrap. This works because start_date <= end_date
	// and if the µs are out of order, then there must be an extra ms.
	if (start_micros > (idx_t)end_micros) {
		end_date.value -= Interval::MICROS_PER_MSEC;
		end_micros += Interval::MICROS_PER_MSEC;
	}

	//	Lunar calendars have uneven numbers of months, so we just diff months, not years
	interval_t result;
	result.months = SubtractField(calendar, UCAL_MONTH, end_date);
	result.days = SubtractField(calendar, UCAL_DATE, end_date);

	auto hour_diff = SubtractField(calendar, UCAL_HOUR_OF_DAY, end_date);
	auto min_diff = SubtractField(calendar, UCAL_MINUTE, end_date);
	auto sec_diff = SubtractField(calendar, UCAL_SECOND, end_date);
	auto ms_diff = SubtractField(calendar, UCAL_MILLISECOND, end_date);
	auto micros_diff = ms_diff * Interval::MICROS_PER_MSEC + (end_micros - start_micros);
	result.micros = Time::FromTime(hour_diff, min_diff, sec_diff, micros_diff).micros;

	return result;
}

struct ICUDateAdd : public ICUDateFunc {

	template <typename TA, typename TR, typename OP>
	static void ExecuteUnary(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 1);

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		auto end_date = Timestamp::GetCurrentTimestamp();
		UnaryExecutor::Execute<TA, TR>(args.data[0], result, args.size(), [&](TA start_date) {
			return OP::template Operation<timestamp_t, TA, TR>(end_date, start_date, calendar.get());
		});
	}

	template <typename TA, typename TR, typename OP>
	inline static ScalarFunction GetUnaryDateFunction(const LogicalTypeId &left_type,
	                                                  const LogicalTypeId &result_type) {
		return ScalarFunction({left_type}, result_type, ExecuteUnary<TA, TR, OP>, Bind);
	}

	template <typename TA, typename TB, typename TR, typename OP>
	static void ExecuteBinary(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		BinaryExecutor::Execute<TA, TB, TR>(args.data[0], args.data[1], result, args.size(), [&](TA left, TB right) {
			return OP::template Operation<TA, TB, TR>(left, right, calendar.get());
		});
	}

	template <typename TA, typename TB, typename TR, typename OP>
	inline static ScalarFunction GetBinaryDateFunction(const LogicalTypeId &left_type, const LogicalTypeId &right_type,
	                                                   const LogicalTypeId &result_type) {
		return ScalarFunction({left_type, right_type}, result_type, ExecuteBinary<TA, TB, TR, OP>, Bind);
	}

	template <typename TA, typename TB, typename OP>
	static ScalarFunction GetDateAddFunction(const LogicalTypeId &left_type, const LogicalTypeId &right_type) {
		return GetBinaryDateFunction<TA, TB, timestamp_t, OP>(left_type, right_type, LogicalType::TIMESTAMP_TZ);
	}

	static void AddDateAddOperators(const string &name, ClientContext &context) {
		//	temporal + interval
		ScalarFunctionSet set(name);
		set.AddFunction(GetDateAddFunction<timestamp_t, interval_t, ICUCalendarAdd>(LogicalType::TIMESTAMP_TZ,
		                                                                            LogicalType::INTERVAL));
		set.AddFunction(GetDateAddFunction<interval_t, timestamp_t, ICUCalendarAdd>(LogicalType::INTERVAL,
		                                                                            LogicalType::TIMESTAMP_TZ));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}

	template <typename TA, typename OP>
	static ScalarFunction GetUnaryAgeFunction(const LogicalTypeId &left_type) {
		return GetUnaryDateFunction<TA, interval_t, OP>(left_type, LogicalType::INTERVAL);
	}

	template <typename TA, typename TB, typename OP>
	static ScalarFunction GetBinaryAgeFunction(const LogicalTypeId &left_type, const LogicalTypeId &right_type) {
		return GetBinaryDateFunction<TA, TB, interval_t, OP>(left_type, right_type, LogicalType::INTERVAL);
	}

	static void AddDateSubOperators(const string &name, ClientContext &context) {
		//	temporal - interval
		ScalarFunctionSet set(name);
		set.AddFunction(GetDateAddFunction<timestamp_t, interval_t, ICUCalendarSub>(LogicalType::TIMESTAMP_TZ,
		                                                                            LogicalType::INTERVAL));

		//	temporal - temporal
		set.AddFunction(GetBinaryAgeFunction<timestamp_t, timestamp_t, ICUCalendarSub>(LogicalType::TIMESTAMP_TZ,
		                                                                               LogicalType::TIMESTAMP_TZ));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}

	static void AddDateAgeFunctions(const string &name, ClientContext &context) {
		//	age(temporal, temporal)
		ScalarFunctionSet set(name);
		set.AddFunction(GetBinaryAgeFunction<timestamp_t, timestamp_t, ICUCalendarAge>(LogicalType::TIMESTAMP_TZ,
		                                                                               LogicalType::TIMESTAMP_TZ));
		set.AddFunction(GetUnaryAgeFunction<timestamp_t, ICUCalendarAge>(LogicalType::TIMESTAMP_TZ));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUDateAddFunctions(ClientContext &context) {
	ICUDateAdd::AddDateAddOperators("+", context);
	ICUDateAdd::AddDateSubOperators("-", context);
	ICUDateAdd::AddDateAgeFunctions("age", context);
}

} // namespace duckdb
