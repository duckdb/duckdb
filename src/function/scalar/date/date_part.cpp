#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

DatePartSpecifier GetDatePartSpecifier(string specifier) {
	specifier = StringUtil::Lower(specifier);
	if (specifier == "year" || specifier == "y" || specifier == "years") {
		return DatePartSpecifier::YEAR;
	} else if (specifier == "month" || specifier == "mon" || specifier == "months" || specifier == "mons") {
		return DatePartSpecifier::MONTH;
	} else if (specifier == "day" || specifier == "days" || specifier == "d") {
		return DatePartSpecifier::DAY;
	} else if (specifier == "decade" || specifier == "decades") {
		return DatePartSpecifier::DECADE;
	} else if (specifier == "century" || specifier == "centuries") {
		return DatePartSpecifier::CENTURY;
	} else if (specifier == "millennium" || specifier == "millenia") {
		return DatePartSpecifier::MILLENNIUM;
	} else if (specifier == "microseconds" || specifier == "microsecond") {
		return DatePartSpecifier::MICROSECONDS;
	} else if (specifier == "milliseconds" || specifier == "millisecond" || specifier == "ms" || specifier == "msec" ||
	           specifier == "msecs") {
		return DatePartSpecifier::MILLISECONDS;
	} else if (specifier == "second" || specifier == "seconds" || specifier == "s") {
		return DatePartSpecifier::SECOND;
	} else if (specifier == "minute" || specifier == "minutes" || specifier == "m") {
		return DatePartSpecifier::MINUTE;
	} else if (specifier == "hour" || specifier == "hours" || specifier == "h") {
		return DatePartSpecifier::HOUR;
	} else if (specifier == "epoch") {
		// seconds since 1970-01-01
		return DatePartSpecifier::EPOCH;
	} else if (specifier == "dow") {
		// day of the week (Sunday = 0, Saturday = 6)
		return DatePartSpecifier::DOW;
	} else if (specifier == "isodow") {
		// isodow (Monday = 1, Sunday = 7)
		return DatePartSpecifier::ISODOW;
	} else if (specifier == "week" || specifier == "weeks" || specifier == "w") {
		// week number
		return DatePartSpecifier::WEEK;
	} else if (specifier == "doy") {
		// day of the year (1-365/366)
		return DatePartSpecifier::DOY;
	} else if (specifier == "quarter") {
		// quarter of the year (1-4)
		return DatePartSpecifier::QUARTER;
	} else {
		throw ConversionException("extract specifier \"%s\" not recognized", specifier);
	}
}

template <class T> static void year_operator(DataChunk &args, ExpressionState &state, Vector &result) {
	int32_t last_year = 0;
	UnaryExecutor::Execute<T, int64_t>(args.data[0], result, args.size(),
	                                   [&](T input) { return Date::ExtractYear(input, &last_year); });
}

template <class T, class OP>
static unique_ptr<BaseStatistics> PropagateDatePartStatistics(vector<unique_ptr<BaseStatistics>> &child_stats) {
	// we can only propagate complex date part stats if the child has stats
	if (!child_stats[0]) {
		return nullptr;
	}
	auto &nstats = (NumericStatistics &)*child_stats[0];
	if (nstats.min.is_null || nstats.max.is_null) {
		return nullptr;
	}
	// run the operator on both the min and the max, this gives us the [min, max] bound
	auto min = nstats.min.GetValueUnsafe<T>();
	auto max = nstats.max.GetValueUnsafe<T>();
	if (min > max) {
		return nullptr;
	}
	auto min_year = OP::template Operation<T, int64_t>(min);
	auto max_year = OP::template Operation<T, int64_t>(max);
	auto result = make_unique<NumericStatistics>(LogicalType::BIGINT, Value::BIGINT(min_year), Value::BIGINT(max_year));
	result->has_null = child_stats[0]->has_null;
	return move(result);
}

template <int64_t MIN, int64_t MAX>
static unique_ptr<BaseStatistics> PropagateSimpleDatePartStatistics(vector<unique_ptr<BaseStatistics>> &child_stats) {
	// we can always propagate simple date part statistics
	// since the min and max can never exceed these bounds
	auto result = make_unique<NumericStatistics>(LogicalType::BIGINT, Value::BIGINT(MIN), Value::BIGINT(MAX));
	if (!child_stats[0]) {
		// if there are no child stats, we don't know
		result->has_null = true;
	} else {
		result->has_null = child_stats[0]->has_null;
	}
	return move(result);
}

struct YearOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractYear(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateDatePartStatistics<T, YearOperator>(child_stats);
	}
};

template <> int64_t YearOperator::Operation(timestamp_t input) {
	return YearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct MonthOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractMonth(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		// min/max of month operator is [1, 12]
		return PropagateSimpleDatePartStatistics<1, 12>(child_stats);
	}
};

template <> int64_t MonthOperator::Operation(timestamp_t input) {
	return MonthOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct DayOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractDay(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		// min/max of day operator is [1, 31]
		return PropagateSimpleDatePartStatistics<1, 31>(child_stats);
	}
};

template <> int64_t DayOperator::Operation(timestamp_t input) {
	return DayOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct DecadeOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractYear(input) / 10;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateDatePartStatistics<T, DecadeOperator>(child_stats);
	}
};

template <> int64_t DecadeOperator::Operation(timestamp_t input) {
	return DecadeOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct CenturyOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return ((Date::ExtractYear(input) - 1) / 100) + 1;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateDatePartStatistics<T, CenturyOperator>(child_stats);
	}
};

template <> int64_t CenturyOperator::Operation(timestamp_t input) {
	return CenturyOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct MilleniumOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return ((Date::ExtractYear(input) - 1) / 1000) + 1;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateDatePartStatistics<T, MilleniumOperator>(child_stats);
	}
};

template <> int64_t MilleniumOperator::Operation(timestamp_t input) {
	return MilleniumOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct QuarterOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return (Date::ExtractMonth(input) - 1) / 3 + 1;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		// min/max of quarter operator is [1, 4]
		return PropagateSimpleDatePartStatistics<1, 4>(child_stats);
	}
};

template <> int64_t QuarterOperator::Operation(timestamp_t input) {
	return QuarterOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct DayOfWeekOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		// day of the week (Sunday = 0, Saturday = 6)
		// turn sunday into 0 by doing mod 7
		return Date::ExtractISODayOfTheWeek(input) % 7;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<0, 6>(child_stats);
	}
};

template <> int64_t DayOfWeekOperator::Operation(timestamp_t input) {
	return DayOfWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct ISODayOfWeekOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		// isodow (Monday = 1, Sunday = 7)
		return Date::ExtractISODayOfTheWeek(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<1, 7>(child_stats);
	}
};

template <> int64_t ISODayOfWeekOperator::Operation(timestamp_t input) {
	return ISODayOfWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct DayOfYearOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractDayOfTheYear(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<1, 366>(child_stats);
	}
};

template <> int64_t DayOfYearOperator::Operation(timestamp_t input) {
	return DayOfYearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct WeekOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractISOWeekNumber(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<1, 54>(child_stats);
	}
};

template <> int64_t WeekOperator::Operation(timestamp_t input) {
	return WeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct YearWeekOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return YearOperator::Operation<TA, TR>(input) * 100 + WeekOperator::Operation<TA, TR>(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateDatePartStatistics<T, YearWeekOperator>(child_stats);
	}
};

struct EpochOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::Epoch(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateDatePartStatistics<T, EpochOperator>(child_stats);
	}
};

template <> int64_t EpochOperator::Operation(timestamp_t input) {
	return Timestamp::GetEpochSeconds(input);
}

struct MicrosecondsOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 0;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<0, 60000000>(child_stats);
	}
};

template <> int64_t MicrosecondsOperator::Operation(timestamp_t input) {
	auto time = Timestamp::GetTime(input);
	// remove everything but the second & microsecond part
	return time % Interval::MICROS_PER_MINUTE;
}

struct MillisecondsOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 0;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<0, 60000>(child_stats);
	}
};

template <> int64_t MillisecondsOperator::Operation(timestamp_t input) {
	return MicrosecondsOperator::Operation<timestamp_t, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

struct SecondsOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 0;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<0, 60>(child_stats);
	}
};

template <> int64_t SecondsOperator::Operation(timestamp_t input) {
	return MicrosecondsOperator::Operation<timestamp_t, int64_t>(input) / Interval::MICROS_PER_SEC;
}

struct MinutesOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 0;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<0, 60>(child_stats);
	}
};

template <> int64_t MinutesOperator::Operation(timestamp_t input) {
	auto time = Timestamp::GetTime(input);
	// remove the hour part, and truncate to minutes
	return (time % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

struct HoursOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 0;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<0, 24>(child_stats);
	}
};

template <> int64_t HoursOperator::Operation(timestamp_t input) {
	return Timestamp::GetTime(input) / Interval::MICROS_PER_HOUR;
}

template <class T> static int64_t extract_element(DatePartSpecifier type, T element) {
	switch (type) {
	case DatePartSpecifier::YEAR:
		return YearOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::MONTH:
		return MonthOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::DAY:
		return DayOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::DECADE:
		return DecadeOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::CENTURY:
		return CenturyOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::MILLENNIUM:
		return MilleniumOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::QUARTER:
		return QuarterOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::DOW:
		return DayOfWeekOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::ISODOW:
		return ISODayOfWeekOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::DOY:
		return DayOfYearOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::WEEK:
		return WeekOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::EPOCH:
		return EpochOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::MICROSECONDS:
		return MicrosecondsOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::MILLISECONDS:
		return MillisecondsOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::SECOND:
		return SecondsOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::MINUTE:
		return MinutesOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::HOUR:
		return HoursOperator::Operation<T, int64_t>(element);
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

struct DatePartOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA specifier, TB date) {
		return extract_element<TB>(GetDatePartSpecifier(specifier.GetString()), date);
	}
};

void AddGenericDatePartOperator(BuiltinFunctions &set, string name, scalar_function_t date_func,
                                scalar_function_t ts_func, function_statistics_t date_stats,
                                function_statistics_t ts_stats) {
	ScalarFunctionSet operator_set(name);
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::DATE}, LogicalType::BIGINT, date_func, false, nullptr, nullptr, date_stats));
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BIGINT, ts_func, false, nullptr, nullptr, ts_stats));
	set.AddFunction(operator_set);
}

template <class OP> static void AddDatePartOperator(BuiltinFunctions &set, string name) {
	AddGenericDatePartOperator(set, name, ScalarFunction::UnaryFunction<date_t, int64_t, OP>,
	                           ScalarFunction::UnaryFunction<timestamp_t, int64_t, OP>,
	                           OP::template PropagateStatistics<date_t>, OP::template PropagateStatistics<timestamp_t>);
}

struct LastDayOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		int32_t yyyy, mm, dd;
		Date::Convert(input, yyyy, mm, dd);
		yyyy += (mm / 12);
		mm %= 12;
		++mm;
		return Date::FromDate(yyyy, mm, 1) - 1;
	}
};

template <> date_t LastDayOperator::Operation(timestamp_t input) {
	return LastDayOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

struct MonthNameOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::MonthNames[MonthOperator::Operation<TA, int64_t>(input) - 1];
	}
};

struct DayNameOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::DayNames[DayOfWeekOperator::Operation<TA, int64_t>(input)];
	}
};

void DatePartFun::RegisterFunction(BuiltinFunctions &set) {
	// register the individual operators
	AddGenericDatePartOperator(set, "year", year_operator<date_t>, year_operator<timestamp_t>,
	                           YearOperator::PropagateStatistics<date_t>,
	                           YearOperator::PropagateStatistics<timestamp_t>);
	AddDatePartOperator<MonthOperator>(set, "month");
	AddDatePartOperator<DayOperator>(set, "day");
	AddDatePartOperator<DecadeOperator>(set, "decade");
	AddDatePartOperator<CenturyOperator>(set, "century");
	AddDatePartOperator<MilleniumOperator>(set, "millenium");
	AddDatePartOperator<QuarterOperator>(set, "quarter");
	AddDatePartOperator<DayOfWeekOperator>(set, "dayofweek");
	AddDatePartOperator<ISODayOfWeekOperator>(set, "isodow");
	AddDatePartOperator<DayOfYearOperator>(set, "dayofyear");
	AddDatePartOperator<WeekOperator>(set, "week");
	AddDatePartOperator<EpochOperator>(set, "epoch");
	AddDatePartOperator<MicrosecondsOperator>(set, "microsecond");
	AddDatePartOperator<MillisecondsOperator>(set, "millisecond");
	AddDatePartOperator<SecondsOperator>(set, "second");
	AddDatePartOperator<MinutesOperator>(set, "minute");
	AddDatePartOperator<HoursOperator>(set, "hour");

	//  register combinations
	AddDatePartOperator<YearWeekOperator>(set, "yearweek");

	//  register various aliases
	AddDatePartOperator<DayOperator>(set, "dayofmonth");
	AddDatePartOperator<DayOfWeekOperator>(set, "weekday");
	AddDatePartOperator<WeekOperator>(set, "weekofyear"); //  Note that WeekOperator is ISO-8601, not US

	//  register the last_day function
	ScalarFunctionSet last_day("last_day");
	last_day.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::DATE,
	                                    ScalarFunction::UnaryFunction<date_t, date_t, LastDayOperator, true>));
	last_day.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::DATE,
	                                    ScalarFunction::UnaryFunction<timestamp_t, date_t, LastDayOperator, true>));
	set.AddFunction(last_day);

	//  register the monthname function
	ScalarFunctionSet monthname("monthname");
	monthname.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::VARCHAR,
	                                     ScalarFunction::UnaryFunction<date_t, string_t, MonthNameOperator, true>));
	monthname.AddFunction(
	    ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::VARCHAR,
	                   ScalarFunction::UnaryFunction<timestamp_t, string_t, MonthNameOperator, true>));
	set.AddFunction(monthname);

	//  register the dayname function
	ScalarFunctionSet dayname("dayname");
	dayname.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::VARCHAR,
	                                   ScalarFunction::UnaryFunction<date_t, string_t, DayNameOperator, true>));
	dayname.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::VARCHAR,
	                                   ScalarFunction::UnaryFunction<timestamp_t, string_t, DayNameOperator, true>));
	set.AddFunction(dayname);

	// finally the actual date_part function
	ScalarFunctionSet date_part("date_part");
	date_part.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::DATE}, LogicalType::BIGINT,
	                   ScalarFunction::BinaryFunction<string_t, date_t, int64_t, DatePartOperator, true>));
	date_part.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, LogicalType::BIGINT,
	                   ScalarFunction::BinaryFunction<string_t, timestamp_t, int64_t, DatePartOperator, true>));
	set.AddFunction(date_part);
	date_part.name = "datepart";
	set.AddFunction(date_part);
}

} // namespace duckdb
