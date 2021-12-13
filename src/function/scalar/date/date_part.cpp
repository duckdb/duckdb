#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"
#include "duckdb/storage/statistics/numeric_statistics.hpp"

namespace duckdb {

bool TryGetDatePartSpecifier(const string &specifier_p, DatePartSpecifier &result) {
	auto specifier = StringUtil::Lower(specifier_p);
	if (specifier == "year" || specifier == "y" || specifier == "years") {
		result = DatePartSpecifier::YEAR;
	} else if (specifier == "month" || specifier == "mon" || specifier == "months" || specifier == "mons") {
		result = DatePartSpecifier::MONTH;
	} else if (specifier == "day" || specifier == "days" || specifier == "d") {
		result = DatePartSpecifier::DAY;
	} else if (specifier == "decade" || specifier == "decades") {
		result = DatePartSpecifier::DECADE;
	} else if (specifier == "century" || specifier == "centuries") {
		result = DatePartSpecifier::CENTURY;
	} else if (specifier == "millennium" || specifier == "millennia") {
		result = DatePartSpecifier::MILLENNIUM;
	} else if (specifier == "microseconds" || specifier == "microsecond") {
		result = DatePartSpecifier::MICROSECONDS;
	} else if (specifier == "milliseconds" || specifier == "millisecond" || specifier == "ms" || specifier == "msec" ||
	           specifier == "msecs") {
		result = DatePartSpecifier::MILLISECONDS;
	} else if (specifier == "second" || specifier == "seconds" || specifier == "s") {
		result = DatePartSpecifier::SECOND;
	} else if (specifier == "minute" || specifier == "minutes" || specifier == "m") {
		result = DatePartSpecifier::MINUTE;
	} else if (specifier == "hour" || specifier == "hours" || specifier == "h") {
		result = DatePartSpecifier::HOUR;
	} else if (specifier == "epoch") {
		// seconds since 1970-01-01
		result = DatePartSpecifier::EPOCH;
	} else if (specifier == "dow" || specifier == "dayofweek") {
		// day of the week (Sunday = 0, Saturday = 6)
		result = DatePartSpecifier::DOW;
	} else if (specifier == "isodow") {
		// isodow (Monday = 1, Sunday = 7)
		result = DatePartSpecifier::ISODOW;
	} else if (specifier == "week" || specifier == "weeks" || specifier == "w") {
		// week number
		result = DatePartSpecifier::WEEK;
	} else if (specifier == "doy" || specifier == "dayofyear") {
		// day of the year (1-365/366)
		result = DatePartSpecifier::DOY;
	} else if (specifier == "quarter" || specifier == "quarters") {
		// quarter of the year (1-4)
		result = DatePartSpecifier::QUARTER;
	} else if (specifier == "yearweek") {
		// Combined year and week YYYYWW
		result = DatePartSpecifier::YEARWEEK;
	} else {
		return false;
	}
	return true;
}

DatePartSpecifier GetDatePartSpecifier(const string &specifier) {
	DatePartSpecifier result;
	if (!TryGetDatePartSpecifier(specifier, result)) {
		throw ConversionException("extract specifier \"%s\" not recognized", specifier);
	}
	return result;
}

template <class T>
static void LastYearFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
	if (child_stats[0]->validity_stats) {
		result->validity_stats = child_stats[0]->validity_stats->Copy();
	}
	return move(result);
}

template <int64_t MIN, int64_t MAX>
static unique_ptr<BaseStatistics> PropagateSimpleDatePartStatistics(vector<unique_ptr<BaseStatistics>> &child_stats) {
	// we can always propagate simple date part statistics
	// since the min and max can never exceed these bounds
	auto result = make_unique<NumericStatistics>(LogicalType::BIGINT, Value::BIGINT(MIN), Value::BIGINT(MAX));
	if (!child_stats[0]) {
		// if there are no child stats, we don't know
		result->validity_stats = make_unique<ValidityStatistics>(true);
	} else if (child_stats[0]->validity_stats) {
		result->validity_stats = child_stats[0]->validity_stats->Copy();
	}
	return move(result);
}

struct DatePart {
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
};

template <>
int64_t DatePart::YearOperator::Operation(timestamp_t input) {
	return YearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::YearOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_YEAR;
}

template <>
int64_t DatePart::YearOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"year\" not recognized");
}

template <>
int64_t DatePart::MonthOperator::Operation(timestamp_t input) {
	return MonthOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::MonthOperator::Operation(interval_t input) {
	return input.months % Interval::MONTHS_PER_YEAR;
}

template <>
int64_t DatePart::MonthOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"month\" not recognized");
}

template <>
int64_t DatePart::DayOperator::Operation(timestamp_t input) {
	return DayOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::DayOperator::Operation(interval_t input) {
	return input.days;
}

template <>
int64_t DatePart::DayOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"day\" not recognized");
}

template <>
int64_t DatePart::DecadeOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_DECADE;
}

template <>
int64_t DatePart::DecadeOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"decade\" not recognized");
}

template <>
int64_t DatePart::CenturyOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_CENTURY;
}

template <>
int64_t DatePart::CenturyOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"century\" not recognized");
}

template <>
int64_t DatePart::MilleniumOperator::Operation(interval_t input) {
	return input.months / Interval::MONTHS_PER_MILLENIUM;
}

template <>
int64_t DatePart::MilleniumOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"millennium\" not recognized");
}

template <>
int64_t DatePart::QuarterOperator::Operation(timestamp_t input) {
	return QuarterOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::QuarterOperator::Operation(interval_t input) {
	return MonthOperator::Operation<interval_t, int64_t>(input) / Interval::MONTHS_PER_QUARTER + 1;
}

template <>
int64_t DatePart::QuarterOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"quarter\" not recognized");
}

template <>
int64_t DatePart::DayOfWeekOperator::Operation(timestamp_t input) {
	return DayOfWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::DayOfWeekOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"dow\" not recognized");
}

template <>
int64_t DatePart::DayOfWeekOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"dow\" not recognized");
}

template <>
int64_t DatePart::ISODayOfWeekOperator::Operation(timestamp_t input) {
	return ISODayOfWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::ISODayOfWeekOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"isodow\" not recognized");
}

template <>
int64_t DatePart::ISODayOfWeekOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"isodow\" not recognized");
}

template <>
int64_t DatePart::DayOfYearOperator::Operation(timestamp_t input) {
	return DayOfYearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::DayOfYearOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"doy\" not recognized");
}

template <>
int64_t DatePart::DayOfYearOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"doy\" not recognized");
}

template <>
int64_t DatePart::WeekOperator::Operation(timestamp_t input) {
	return WeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

template <>
int64_t DatePart::WeekOperator::Operation(interval_t input) {
	throw NotImplementedException("interval units \"week\" not recognized");
}

template <>
int64_t DatePart::WeekOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"week\" not recognized");
}

template <>
int64_t DatePart::YearWeekOperator::Operation(dtime_t input) {
	throw NotImplementedException("\"time\" units \"yearweek\" not recognized");
}

template <>
int64_t DatePart::MicrosecondsOperator::Operation(timestamp_t input) {
	auto time = Timestamp::GetTime(input);
	// remove everything but the second & microsecond part
	return time.micros % Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MicrosecondsOperator::Operation(interval_t input) {
	// remove everything but the second & microsecond part
	return input.micros % Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MicrosecondsOperator::Operation(dtime_t input) {
	// remove everything but the second & microsecond part
	return input.micros % Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MillisecondsOperator::Operation(timestamp_t input) {
	return MicrosecondsOperator::Operation<timestamp_t, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DatePart::MillisecondsOperator::Operation(interval_t input) {
	return MicrosecondsOperator::Operation<interval_t, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DatePart::MillisecondsOperator::Operation(dtime_t input) {
	return MicrosecondsOperator::Operation<dtime_t, int64_t>(input) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DatePart::SecondsOperator::Operation(timestamp_t input) {
	return MicrosecondsOperator::Operation<timestamp_t, int64_t>(input) / Interval::MICROS_PER_SEC;
}

template <>
int64_t DatePart::SecondsOperator::Operation(interval_t input) {
	return MicrosecondsOperator::Operation<interval_t, int64_t>(input) / Interval::MICROS_PER_SEC;
}

template <>
int64_t DatePart::SecondsOperator::Operation(dtime_t input) {
	return MicrosecondsOperator::Operation<dtime_t, int64_t>(input) / Interval::MICROS_PER_SEC;
}

template <>
int64_t DatePart::MinutesOperator::Operation(timestamp_t input) {
	auto time = Timestamp::GetTime(input);
	// remove the hour part, and truncate to minutes
	return (time.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MinutesOperator::Operation(interval_t input) {
	// remove the hour part, and truncate to minutes
	return (input.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::MinutesOperator::Operation(dtime_t input) {
	// remove the hour part, and truncate to minutes
	return (input.micros % Interval::MICROS_PER_HOUR) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DatePart::HoursOperator::Operation(timestamp_t input) {
	return Timestamp::GetTime(input).micros / Interval::MICROS_PER_HOUR;
}

template <>
int64_t DatePart::HoursOperator::Operation(interval_t input) {
	return input.micros / Interval::MICROS_PER_HOUR;
}

template <>
int64_t DatePart::HoursOperator::Operation(dtime_t input) {
	return input.micros / Interval::MICROS_PER_HOUR;
}

template <>
int64_t DatePart::EpochOperator::Operation(timestamp_t input) {
	return Timestamp::GetEpochSeconds(input);
}

template <>
int64_t DatePart::EpochOperator::Operation(interval_t input) {
	int64_t interval_years = input.months / Interval::MONTHS_PER_YEAR;
	int64_t interval_days;
	interval_days = Interval::DAYS_PER_YEAR * interval_years;
	interval_days += Interval::DAYS_PER_MONTH * (input.months % Interval::MONTHS_PER_YEAR);
	interval_days += input.days;
	int64_t interval_epoch;
	interval_epoch = interval_days * Interval::SECS_PER_DAY;
	// we add 0.25 days per year to sort of account for leap days
	interval_epoch += interval_years * (Interval::SECS_PER_DAY / 4);
	interval_epoch += input.micros / Interval::MICROS_PER_SEC;
	return interval_epoch;
}

template <>
int64_t DatePart::EpochOperator::Operation(dtime_t input) {
	return input.micros / Interval::MICROS_PER_SEC;
}

template <>
unique_ptr<BaseStatistics>
DatePart::EpochOperator::PropagateStatistics<dtime_t>(ClientContext &context, BoundFunctionExpression &expr,
                                                      FunctionData *bind_data,
                                                      vector<unique_ptr<BaseStatistics>> &child_stats) {
	// time seconds range over a single day
	return PropagateSimpleDatePartStatistics<0, 86400>(child_stats);
}

template <class T>
static int64_t ExtractElement(DatePartSpecifier type, T element) {
	switch (type) {
	case DatePartSpecifier::YEAR:
		return DatePart::YearOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MONTH:
		return DatePart::MonthOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::DAY:
		return DatePart::DayOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::DECADE:
		return DatePart::DecadeOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::CENTURY:
		return DatePart::CenturyOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MILLENNIUM:
		return DatePart::MilleniumOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::QUARTER:
		return DatePart::QuarterOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::DOW:
		return DatePart::DayOfWeekOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::ISODOW:
		return DatePart::ISODayOfWeekOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::DOY:
		return DatePart::DayOfYearOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::WEEK:
		return DatePart::WeekOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::YEARWEEK:
		return DatePart::YearWeekOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::EPOCH:
		return DatePart::EpochOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MICROSECONDS:
		return DatePart::MicrosecondsOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MILLISECONDS:
		return DatePart::MillisecondsOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::SECOND:
		return DatePart::SecondsOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::MINUTE:
		return DatePart::MinutesOperator::template Operation<T, int64_t>(element);
	case DatePartSpecifier::HOUR:
		return DatePart::HoursOperator::template Operation<T, int64_t>(element);
	default:
		throw NotImplementedException("Specifier type not implemented for DATEPART");
	}
}

struct DatePartBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA specifier, TB date) {
		return ExtractElement<TB>(GetDatePartSpecifier(specifier.GetString()), date);
	}
};

template <typename T>
static void DatePartFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto &part_arg = args.data[0];
	auto &date_arg = args.data[1];

	BinaryExecutor::ExecuteStandard<string_t, T, int64_t, DatePartBinaryOperator>(part_arg, date_arg, result,
	                                                                              args.size());
}

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
	    ScalarFunction::UnaryFunction<dtime_t, int64_t, OP>, OP::template PropagateStatistics<date_t>,
	    OP::template PropagateStatistics<timestamp_t>, OP::template PropagateStatistics<dtime_t>);
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
		return Date::MONTH_NAMES[DatePart::MonthOperator::Operation<TA, int64_t>(input) - 1];
	}
};

struct DayNameOperator {
	template <class TA, class TR>
	static inline TR Operation(TA input) {
		return Date::DAY_NAMES[DatePart::DayOfWeekOperator::Operation<TA, int64_t>(input)];
	}
};

void DatePartFun::RegisterFunction(BuiltinFunctions &set) {
	// register the individual operators
	AddGenericDatePartOperator(set, "year", LastYearFunction<date_t>, LastYearFunction<timestamp_t>,
	                           ScalarFunction::UnaryFunction<interval_t, int64_t, DatePart::YearOperator>,
	                           DatePart::YearOperator::PropagateStatistics<date_t>,
	                           DatePart::YearOperator::PropagateStatistics<timestamp_t>);
	AddDatePartOperator<DatePart::MonthOperator>(set, "month");
	AddDatePartOperator<DatePart::DayOperator>(set, "day");
	AddDatePartOperator<DatePart::DecadeOperator>(set, "decade");
	AddDatePartOperator<DatePart::CenturyOperator>(set, "century");
	AddDatePartOperator<DatePart::MilleniumOperator>(set, "millennium");
	AddDatePartOperator<DatePart::QuarterOperator>(set, "quarter");
	AddDatePartOperator<DatePart::DayOfWeekOperator>(set, "dayofweek");
	AddDatePartOperator<DatePart::ISODayOfWeekOperator>(set, "isodow");
	AddDatePartOperator<DatePart::DayOfYearOperator>(set, "dayofyear");
	AddDatePartOperator<DatePart::WeekOperator>(set, "week");
	AddTimePartOperator<DatePart::EpochOperator>(set, "epoch");
	AddTimePartOperator<DatePart::MicrosecondsOperator>(set, "microsecond");
	AddTimePartOperator<DatePart::MillisecondsOperator>(set, "millisecond");
	AddTimePartOperator<DatePart::SecondsOperator>(set, "second");
	AddTimePartOperator<DatePart::MinutesOperator>(set, "minute");
	AddTimePartOperator<DatePart::HoursOperator>(set, "hour");

	//  register combinations
	AddDatePartOperator<DatePart::YearWeekOperator>(set, "yearweek");

	//  register various aliases
	AddDatePartOperator<DatePart::DayOperator>(set, "dayofmonth");
	AddDatePartOperator<DatePart::DayOfWeekOperator>(set, "weekday");
	AddDatePartOperator<DatePart::WeekOperator>(set, "weekofyear"); //  Note that WeekOperator is ISO-8601, not US

	//  register the last_day function
	ScalarFunctionSet last_day("last_day");
	last_day.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::DATE,
	                                    ScalarFunction::UnaryFunction<date_t, date_t, LastDayOperator>));
	last_day.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::DATE,
	                                    ScalarFunction::UnaryFunction<timestamp_t, date_t, LastDayOperator>));
	set.AddFunction(last_day);

	//  register the monthname function
	ScalarFunctionSet monthname("monthname");
	monthname.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::VARCHAR,
	                                     ScalarFunction::UnaryFunction<date_t, string_t, MonthNameOperator>));
	monthname.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::VARCHAR,
	                                     ScalarFunction::UnaryFunction<timestamp_t, string_t, MonthNameOperator>));
	set.AddFunction(monthname);

	//  register the dayname function
	ScalarFunctionSet dayname("dayname");
	dayname.AddFunction(ScalarFunction({LogicalType::DATE}, LogicalType::VARCHAR,
	                                   ScalarFunction::UnaryFunction<date_t, string_t, DayNameOperator>));
	dayname.AddFunction(ScalarFunction({LogicalType::TIMESTAMP}, LogicalType::VARCHAR,
	                                   ScalarFunction::UnaryFunction<timestamp_t, string_t, DayNameOperator>));
	set.AddFunction(dayname);

	// finally the actual date_part function
	ScalarFunctionSet date_part("date_part");
	date_part.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::DATE}, LogicalType::BIGINT, DatePartFunction<date_t>));
	date_part.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, LogicalType::BIGINT,
	                                     DatePartFunction<timestamp_t>));
	date_part.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::TIME}, LogicalType::BIGINT, DatePartFunction<dtime_t>));
	date_part.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTERVAL}, LogicalType::BIGINT,
	                                     DatePartFunction<interval_t>));
	set.AddFunction(date_part);
	date_part.name = "datepart";
	set.AddFunction(date_part);
}

} // namespace duckdb
