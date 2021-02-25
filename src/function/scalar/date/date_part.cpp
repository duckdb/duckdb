#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

// hack around type equivalences
struct DTime {
	dtime_t time;
};

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

template <class T>
static void LastYearOperator(DataChunk &args, ExpressionState &state, Vector &result) {
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
	auto min_part = OP::template Operation<T, int64_t>(min);
	auto max_part = OP::template Operation<T, int64_t>(max);
	auto result = make_unique<NumericStatistics>(LogicalType::BIGINT, Value::BIGINT(min_part), Value::BIGINT(max_part));
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
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Date::ExtractYear(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateDatePartStatistics<T, YearOperator>(child_stats);
	}
};

template <>
int64_t YearOperator::Operation(timestamp_t input) {
	return YearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t YearOperator::Operation(DTime input) {
	throw NotImplementedException("\"time\" units \"year\" not recognized");
}

template <>
int64_t YearOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_YEAR;
}

struct MonthOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
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

template <>
int64_t MonthOperator::Operation(timestamp_t input) {
	return MonthOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t MonthOperator::Operation(DTime input) {
	throw NotImplementedException("\"time\" units \"month\" not recognized");
}

template <>
int64_t MonthOperator::Operation(interval_t input) {
	return input.months % Interval::MONTHS_PER_YEAR;
}

template <>
unique_ptr<BaseStatistics>
MonthOperator::PropagateStatistics<interval_t>(ClientContext &context, BoundFunctionExpression &expr,
                                               FunctionData *bind_data,
                                               vector<unique_ptr<BaseStatistics>> &child_stats) {
	// interval months range from 0-11
	return PropagateSimpleDatePartStatistics<0, 11>(child_stats);
}

struct DayOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
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

template <>
int64_t DayOperator::Operation(timestamp_t input) {
	return DayOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DayOperator::Operation(DTime input) {
	throw NotImplementedException("\"time\" units \"month\" not recognized");
}

template <>
int64_t DayOperator::Operation(interval_t input) {
	return input.days;
}

struct DecadeOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return YearOperator::Operation<TA, TR>(input) / 10;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateDatePartStatistics<T, DecadeOperator>(child_stats);
	}
};

template <>
int64_t DecadeOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_DECADE;
}

template <>
int64_t DecadeOperator::Operation(DTime input) {
	throw NotImplementedException("\"time\" units \"decade\" not recognized");
}

struct CenturyOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return ((YearOperator::Operation<TA, TR>(input) - 1) / 100) + 1;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateDatePartStatistics<T, CenturyOperator>(child_stats);
	}
};

template <>
int64_t CenturyOperator::Operation(DTime input) {
	throw NotImplementedException("\"time\" units \"century\" not recognized");
}

template <>
int64_t CenturyOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_CENTURY;
}

struct MilleniumOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return ((YearOperator::Operation<TA, TR>(input) - 1) / 1000) + 1;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateDatePartStatistics<T, MilleniumOperator>(child_stats);
	}
};

template <>
int64_t MilleniumOperator::Operation(DTime input) {
	throw NotImplementedException("\"time\" units \"millennium\" not recognized");
}

template <>
int64_t MilleniumOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_MILLENIUM;
}

struct QuarterOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return (Date::ExtractMonth(input) - 1) / Interval::MONTHS_PER_QUARTER + 1;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		// min/max of quarter operator is [1, 4]
		return PropagateSimpleDatePartStatistics<1, 4>(child_stats);
	}
};

template <>
int64_t QuarterOperator::Operation(timestamp_t input) {
	return QuarterOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t QuarterOperator::Operation(DTime input) {
	throw NotImplementedException("\"time\" units \"quarter\" not recognized");
}

template <>
int64_t QuarterOperator::Operation(interval_t input) {
	return MonthOperator::Operation<interval_t, int64_t>(input) / Interval::MONTHS_PER_QUARTER + 1;
}

template <>
unique_ptr<BaseStatistics>
QuarterOperator::PropagateStatistics<interval_t>(ClientContext &context, BoundFunctionExpression &expr,
                                                 FunctionData *bind_data,
                                                 vector<unique_ptr<BaseStatistics>> &child_stats) {
	// negative interval quarters range from -2 to 4
	return PropagateSimpleDatePartStatistics<-2, 4>(child_stats);
}

struct DayOfWeekOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
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

template <>
int64_t DayOfWeekOperator::Operation(timestamp_t input) {
	return DayOfWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DayOfWeekOperator::Operation(DTime input) {
	throw NotImplementedException("\"time\" units \"dow\" not recognized");
}

template <>
int64_t DayOfWeekOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"dow\" not recognized");
}

struct ISODayOfWeekOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
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

template <>
int64_t ISODayOfWeekOperator::Operation(timestamp_t input) {
	return ISODayOfWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t ISODayOfWeekOperator::Operation(DTime input) {
	throw NotImplementedException("\"time\" units \"isodow\" not recognized");
}

template <>
int64_t ISODayOfWeekOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"isodow\" not recognized");
}

struct DayOfYearOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Date::ExtractDayOfTheYear(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<1, 366>(child_stats);
	}
};

template <>
int64_t DayOfYearOperator::Operation(timestamp_t input) {
	return DayOfYearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DayOfYearOperator::Operation(DTime input) {
	throw NotImplementedException("\"time\" units \"doy\" not recognized");
}

template <>
int64_t DayOfYearOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"doy\" not recognized");
}

struct WeekOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Date::ExtractISOWeekNumber(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<1, 54>(child_stats);
	}
};

template <>
int64_t WeekOperator::Operation(timestamp_t input) {
	return WeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t WeekOperator::Operation(DTime input) {
	throw NotImplementedException("\"time\" units \"week\" not recognized");
}

template <>
int64_t WeekOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"week\" not recognized");
}

struct YearWeekOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return YearOperator::Operation<TA, TR>(input) * 100 + WeekOperator::Operation<TA, TR>(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateDatePartStatistics<T, YearWeekOperator>(child_stats);
	}
};

struct MicrosecondsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return 0;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<0, 60000000>(child_stats);
	}
};

template <>
int64_t MicrosecondsOperator::Operation(timestamp_t input) {
	auto time = Timestamp::GetTime(input);
	// remove everything but the second & microsecond part
	return time % Interval::MICROS_PER_MINUTE;
}

template <>
int64_t MicrosecondsOperator::Operation(DTime input) {
	// remove everything but the second & microsecond part
	return input.time % Interval::MICROS_PER_MINUTE;
}

template <>
int64_t MicrosecondsOperator::Operation(interval_t input) {
	// remove everything but the second & microsecond part
	return input.micros;
}

struct MillisecondsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return 0;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<0, 60000>(child_stats);
	}
};

template <>
int64_t MillisecondsOperator::Operation(timestamp_t input) {
	return MicrosecondsOperator::Operation<timestamp_t, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t MillisecondsOperator::Operation(DTime input) {
	return MicrosecondsOperator::Operation<DTime, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t MillisecondsOperator::Operation(interval_t input) {
	return MicrosecondsOperator::Operation<interval_t, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

struct SecondsOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return 0;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<0, 60>(child_stats);
	}
};

template <>
int64_t SecondsOperator::Operation(timestamp_t input) {
	return MicrosecondsOperator::Operation<timestamp_t, int64_t>(input) / Interval::MICROS_PER_SEC;
}

template <>
int64_t SecondsOperator::Operation(DTime input) {
	return MicrosecondsOperator::Operation<DTime, int64_t>(input) / Interval::MICROS_PER_SEC;
}

template <>
int64_t SecondsOperator::Operation(interval_t input) {
	return MicrosecondsOperator::Operation<interval_t, int64_t>(input) / Interval::MICROS_PER_SEC;
}

struct MinutesOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return 0;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<0, 60>(child_stats);
	}
};

template <>
int64_t MinutesOperator::Operation(timestamp_t input) {
	auto time = Timestamp::GetTime(input);
	// remove the hour part, and truncate to minutes
	return (time % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t MinutesOperator::Operation(DTime input) {
	// remove the hour part, and truncate to minutes
	return (input.time % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t MinutesOperator::Operation(interval_t input) {
	// remove the hour part, and truncate to minutes
	return (input.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

struct HoursOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return 0;
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateSimpleDatePartStatistics<0, 24>(child_stats);
	}
};

template <>
int64_t HoursOperator::Operation(timestamp_t input) {
	return Timestamp::GetTime(input) / Interval::MICROS_PER_HOUR;
}

template <>
int64_t HoursOperator::Operation(DTime input) {
	return input.time / Interval::MICROS_PER_HOUR;
}

template <>
int64_t HoursOperator::Operation(interval_t input) {
	return input.micros / Interval::MICROS_PER_HOUR;
}

struct EpochOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Date::Epoch(input);
	}

	template <class T>
	static unique_ptr<BaseStatistics> PropagateStatistics(ClientContext &context, BoundFunctionExpression &expr,
	                                                      FunctionData *bind_data,
	                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
		return PropagateDatePartStatistics<T, EpochOperator>(child_stats);
	}
};

template <>
int64_t EpochOperator::Operation(timestamp_t input) {
	return Timestamp::GetEpochSeconds(input);
}

template <>
int64_t EpochOperator::Operation(DTime input) {
	return SecondsOperator::Operation<DTime, int64_t>(input);
}

template <>
unique_ptr<BaseStatistics>
EpochOperator::PropagateStatistics<DTime>(ClientContext &context, BoundFunctionExpression &expr,
                                          FunctionData *bind_data, vector<unique_ptr<BaseStatistics>> &child_stats) {
	return PropagateSimpleDatePartStatistics<0, 86400>(child_stats);
}

template <>
int64_t EpochOperator::Operation(interval_t input) {
	auto secs = SecondsOperator::Operation<interval_t, int64_t>(input);
	return (input.months * Interval::DAYS_PER_MONTH + input.days) * Interval::SECS_PER_DAY + secs;
}

template <class T>
static int64_t ExtractElement(DatePartSpecifier type, T element) {
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
	template <class TA, class TB, class TR>
	static inline TR Operation(TA specifier, TB date) {
		return ExtractElement<TB>(GetDatePartSpecifier(specifier.GetString()), date);
	}
};

void AddGenericDatePartOperator(BuiltinFunctions &set, const string &name, scalar_function_t date_func,
                                scalar_function_t ts_func, scalar_function_t interval_func,
                                function_statistics_t date_stats, function_statistics_t ts_stats) {
	ScalarFunctionSet operator_set(name);
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::DATE}, LogicalType::BIGINT, move(date_func), false, nullptr, nullptr, date_stats));
	operator_set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BIGINT, move(ts_func), false,
	                                        nullptr, nullptr, ts_stats));
	operator_set.AddFunction(ScalarFunction({LogicalType::INTERVAL}, LogicalType::BIGINT, move(interval_func)));
	set.AddFunction(operator_set);
}

template <class OP>
static void AddDatePartOperator(BuiltinFunctions &set, string name) {
	AddGenericDatePartOperator(set, name, ScalarFunction::UnaryFunction<date_t, int64_t, OP>,
	                           ScalarFunction::UnaryFunction<timestamp_t, int64_t, OP>,
	                           ScalarFunction::UnaryFunction<interval_t, int64_t, OP>,
	                           OP::template PropagateStatistics<date_t>, OP::template PropagateStatistics<timestamp_t>);
}

void AddGenericTimePartOperator(BuiltinFunctions &set, const string &name, scalar_function_t date_func,
                                scalar_function_t ts_func, scalar_function_t interval_func, scalar_function_t time_func,
                                function_statistics_t date_stats, function_statistics_t ts_stats,
                                function_statistics_t time_stats) {
	ScalarFunctionSet operator_set(name);
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::DATE}, LogicalType::BIGINT, move(date_func), false, nullptr, nullptr, date_stats));
	operator_set.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::BIGINT, move(ts_func), false,
	                                        nullptr, nullptr, ts_stats));
	operator_set.AddFunction(ScalarFunction({LogicalType::INTERVAL}, LogicalType::BIGINT, move(interval_func)));
	operator_set.AddFunction(
	    ScalarFunction({LogicalType::TIME}, LogicalType::BIGINT, move(time_func), false, nullptr, nullptr, time_stats));
	set.AddFunction(operator_set);
}

template <class OP>
static void AddTimePartOperator(BuiltinFunctions &set, string name) {
	AddGenericTimePartOperator(
	    set, name, ScalarFunction::UnaryFunction<date_t, int64_t, OP>,
	    ScalarFunction::UnaryFunction<timestamp_t, int64_t, OP>, ScalarFunction::UnaryFunction<interval_t, int64_t, OP>,
	    ScalarFunction::UnaryFunction<DTime, int64_t, OP>, OP::template PropagateStatistics<date_t>,
	    OP::template PropagateStatistics<timestamp_t>, OP::template PropagateStatistics<DTime>);
}

struct LastDayOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		int32_t yyyy, mm, dd;
		Date::Convert(input, yyyy, mm, dd);
		yyyy += (mm / 12);
		mm %= 12;
		++mm;
		return Date::FromDate(yyyy, mm, 1) - 1;
	}
};

template <>
date_t LastDayOperator::Operation(timestamp_t input) {
	return LastDayOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

struct MonthNameOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Date::MONTH_NAMES[MonthOperator::Operation<TA, int64_t>(input) - 1];
	}
};

struct DayNameOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Date::DAY_NAMES[DayOfWeekOperator::Operation<TA, int64_t>(input)];
	}
};

void DatePartFun::RegisterFunction(BuiltinFunctions &set) {
	// register the individual operators
	AddGenericDatePartOperator(set, "year", LastYearOperator<date_t>, LastYearOperator<timestamp_t>,
	                           ScalarFunction::UnaryFunction<interval_t, int64_t, YearOperator>,
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
	AddTimePartOperator<EpochOperator>(set, "epoch");
	AddTimePartOperator<MicrosecondsOperator>(set, "microsecond");
	AddTimePartOperator<MillisecondsOperator>(set, "millisecond");
	AddTimePartOperator<SecondsOperator>(set, "second");
	AddTimePartOperator<MinutesOperator>(set, "minute");
	AddTimePartOperator<HoursOperator>(set, "hour");

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
	date_part.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::TIME}, LogicalType::BIGINT,
	                   ScalarFunction::BinaryFunction<string_t, DTime, int64_t, DatePartOperator, true>));
	date_part.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::INTERVAL}, LogicalType::BIGINT,
	                   ScalarFunction::BinaryFunction<string_t, interval_t, int64_t, DatePartOperator, true>));
	set.AddFunction(date_part);
	date_part.name = "datepart";
	set.AddFunction(date_part);
}

} // namespace duckdb
