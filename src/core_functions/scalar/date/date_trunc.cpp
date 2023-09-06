#include "duckdb/core_functions/scalar/date_functions.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

struct DateTrunc {
	template <class TA, class TR, class OP>
	static inline TR UnaryFunction(TA input) {
		if (Value::IsFinite(input)) {
			return OP::template Operation<TA, TR>(input);
		} else {
			return Cast::template Operation<TA, TR>(input);
		}
	}

	template <class TA, class TR, class OP>
	static inline void UnaryExecute(Vector &left, Vector &result, idx_t count) {
		UnaryExecutor::Execute<TA, TR>(left, result, count, UnaryFunction<TA, TR, OP>);
	}

	struct MillenniumOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::FromDate((Date::ExtractYear(input) / 1000) * 1000, 1, 1);
		}
	};

	struct CenturyOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::FromDate((Date::ExtractYear(input) / 100) * 100, 1, 1);
		}
	};

	struct DecadeOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::FromDate((Date::ExtractYear(input) / 10) * 10, 1, 1);
		}
	};

	struct YearOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::FromDate(Date::ExtractYear(input), 1, 1);
		}
	};

	struct QuarterOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t yyyy, mm, dd;
			Date::Convert(input, yyyy, mm, dd);
			mm = 1 + (((mm - 1) / 3) * 3);
			return Date::FromDate(yyyy, mm, 1);
		}
	};

	struct MonthOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::FromDate(Date::ExtractYear(input), Date::ExtractMonth(input), 1);
		}
	};

	struct WeekOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return Date::GetMondayOfCurrentWeek(input);
		}
	};

	struct ISOYearOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			date_t date = Date::GetMondayOfCurrentWeek(input);
			date.days -= (Date::ExtractISOWeekNumber(date) - 1) * Interval::DAYS_PER_WEEK;

			return date;
		}
	};

	struct DayOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return input;
		}
	};

	struct HourOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t hour, min, sec, micros;
			date_t date;
			dtime_t time;
			Timestamp::Convert(input, date, time);
			Time::Convert(time, hour, min, sec, micros);
			return Timestamp::FromDatetime(date, Time::FromTime(hour, 0, 0, 0));
		}
	};

	struct MinuteOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t hour, min, sec, micros;
			date_t date;
			dtime_t time;
			Timestamp::Convert(input, date, time);
			Time::Convert(time, hour, min, sec, micros);
			return Timestamp::FromDatetime(date, Time::FromTime(hour, min, 0, 0));
		}
	};

	struct SecondOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t hour, min, sec, micros;
			date_t date;
			dtime_t time;
			Timestamp::Convert(input, date, time);
			Time::Convert(time, hour, min, sec, micros);
			return Timestamp::FromDatetime(date, Time::FromTime(hour, min, sec, 0));
		}
	};

	struct MillisecondOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t hour, min, sec, micros;
			date_t date;
			dtime_t time;
			Timestamp::Convert(input, date, time);
			Time::Convert(time, hour, min, sec, micros);
			micros -= micros % Interval::MICROS_PER_MSEC;
			return Timestamp::FromDatetime(date, Time::FromTime(hour, min, sec, micros));
		}
	};

	struct MicrosecondOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			return input;
		}
	};
};

// DATE specialisations
template <>
date_t DateTrunc::MillenniumOperator::Operation(timestamp_t input) {
	return MillenniumOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::MillenniumOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(MillenniumOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::MillenniumOperator::Operation(timestamp_t input) {
	return MillenniumOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::CenturyOperator::Operation(timestamp_t input) {
	return CenturyOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::CenturyOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(CenturyOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::CenturyOperator::Operation(timestamp_t input) {
	return CenturyOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::DecadeOperator::Operation(timestamp_t input) {
	return DecadeOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::DecadeOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(DecadeOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::DecadeOperator::Operation(timestamp_t input) {
	return DecadeOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::YearOperator::Operation(timestamp_t input) {
	return YearOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::YearOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(YearOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::YearOperator::Operation(timestamp_t input) {
	return YearOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::QuarterOperator::Operation(timestamp_t input) {
	return QuarterOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::QuarterOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(QuarterOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::QuarterOperator::Operation(timestamp_t input) {
	return QuarterOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::MonthOperator::Operation(timestamp_t input) {
	return MonthOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::MonthOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(MonthOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::MonthOperator::Operation(timestamp_t input) {
	return MonthOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::WeekOperator::Operation(timestamp_t input) {
	return WeekOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::WeekOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(WeekOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::WeekOperator::Operation(timestamp_t input) {
	return WeekOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::ISOYearOperator::Operation(timestamp_t input) {
	return ISOYearOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::ISOYearOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(ISOYearOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::ISOYearOperator::Operation(timestamp_t input) {
	return ISOYearOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::DayOperator::Operation(timestamp_t input) {
	return DayOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::DayOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(DayOperator::Operation<date_t, date_t>(input), dtime_t(0));
}

template <>
timestamp_t DateTrunc::DayOperator::Operation(timestamp_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
date_t DateTrunc::HourOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, date_t>(input);
}

template <>
timestamp_t DateTrunc::HourOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
date_t DateTrunc::HourOperator::Operation(timestamp_t input) {
	return Timestamp::GetDate(HourOperator::Operation<timestamp_t, timestamp_t>(input));
}

template <>
date_t DateTrunc::MinuteOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, date_t>(input);
}

template <>
timestamp_t DateTrunc::MinuteOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
date_t DateTrunc::MinuteOperator::Operation(timestamp_t input) {
	return Timestamp::GetDate(HourOperator::Operation<timestamp_t, timestamp_t>(input));
}

template <>
date_t DateTrunc::SecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, date_t>(input);
}

template <>
timestamp_t DateTrunc::SecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
date_t DateTrunc::SecondOperator::Operation(timestamp_t input) {
	return Timestamp::GetDate(DayOperator::Operation<timestamp_t, timestamp_t>(input));
}

template <>
date_t DateTrunc::MillisecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, date_t>(input);
}

template <>
timestamp_t DateTrunc::MillisecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
date_t DateTrunc::MillisecondOperator::Operation(timestamp_t input) {
	return Timestamp::GetDate(MillisecondOperator::Operation<timestamp_t, timestamp_t>(input));
}

template <>
date_t DateTrunc::MicrosecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, date_t>(input);
}

template <>
timestamp_t DateTrunc::MicrosecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
date_t DateTrunc::MicrosecondOperator::Operation(timestamp_t input) {
	return Timestamp::GetDate(MicrosecondOperator::Operation<timestamp_t, timestamp_t>(input));
}

// INTERVAL specialisations
template <>
interval_t DateTrunc::MillenniumOperator::Operation(interval_t input) {
	input.days = 0;
	input.micros = 0;
	input.months = (input.months / Interval::MONTHS_PER_MILLENIUM) * Interval::MONTHS_PER_MILLENIUM;
	return input;
}

template <>
interval_t DateTrunc::CenturyOperator::Operation(interval_t input) {
	input.days = 0;
	input.micros = 0;
	input.months = (input.months / Interval::MONTHS_PER_CENTURY) * Interval::MONTHS_PER_CENTURY;
	return input;
}

template <>
interval_t DateTrunc::DecadeOperator::Operation(interval_t input) {
	input.days = 0;
	input.micros = 0;
	input.months = (input.months / Interval::MONTHS_PER_DECADE) * Interval::MONTHS_PER_DECADE;
	return input;
}

template <>
interval_t DateTrunc::YearOperator::Operation(interval_t input) {
	input.days = 0;
	input.micros = 0;
	input.months = (input.months / Interval::MONTHS_PER_YEAR) * Interval::MONTHS_PER_YEAR;
	return input;
}

template <>
interval_t DateTrunc::QuarterOperator::Operation(interval_t input) {
	input.days = 0;
	input.micros = 0;
	input.months = (input.months / Interval::MONTHS_PER_QUARTER) * Interval::MONTHS_PER_QUARTER;
	return input;
}

template <>
interval_t DateTrunc::MonthOperator::Operation(interval_t input) {
	input.days = 0;
	input.micros = 0;
	return input;
}

template <>
interval_t DateTrunc::WeekOperator::Operation(interval_t input) {
	input.micros = 0;
	input.days = (input.days / Interval::DAYS_PER_WEEK) * Interval::DAYS_PER_WEEK;
	return input;
}

template <>
interval_t DateTrunc::ISOYearOperator::Operation(interval_t input) {
	return YearOperator::Operation<interval_t, interval_t>(input);
}

template <>
interval_t DateTrunc::DayOperator::Operation(interval_t input) {
	input.micros = 0;
	return input;
}

template <>
interval_t DateTrunc::HourOperator::Operation(interval_t input) {
	input.micros = (input.micros / Interval::MICROS_PER_HOUR) * Interval::MICROS_PER_HOUR;
	return input;
}

template <>
interval_t DateTrunc::MinuteOperator::Operation(interval_t input) {
	input.micros = (input.micros / Interval::MICROS_PER_MINUTE) * Interval::MICROS_PER_MINUTE;
	return input;
}

template <>
interval_t DateTrunc::SecondOperator::Operation(interval_t input) {
	input.micros = (input.micros / Interval::MICROS_PER_SEC) * Interval::MICROS_PER_SEC;
	return input;
}

template <>
interval_t DateTrunc::MillisecondOperator::Operation(interval_t input) {
	input.micros = (input.micros / Interval::MICROS_PER_MSEC) * Interval::MICROS_PER_MSEC;
	return input;
}

template <>
interval_t DateTrunc::MicrosecondOperator::Operation(interval_t input) {
	return input;
}

template <class TA, class TR>
static TR TruncateElement(DatePartSpecifier type, TA element) {
	if (!Value::IsFinite(element)) {
		return Cast::template Operation<TA, TR>(element);
	}

	switch (type) {
	case DatePartSpecifier::MILLENNIUM:
		return DateTrunc::MillenniumOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::CENTURY:
		return DateTrunc::CenturyOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::DECADE:
		return DateTrunc::DecadeOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::YEAR:
		return DateTrunc::YearOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::QUARTER:
		return DateTrunc::QuarterOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::MONTH:
		return DateTrunc::MonthOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		return DateTrunc::WeekOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::ISOYEAR:
		return DateTrunc::ISOYearOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		return DateTrunc::DayOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::HOUR:
		return DateTrunc::HourOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::MINUTE:
		return DateTrunc::MinuteOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		return DateTrunc::SecondOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::MILLISECONDS:
		return DateTrunc::MillisecondOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::MICROSECONDS:
		return DateTrunc::MicrosecondOperator::Operation<TA, TR>(element);
	default:
		throw NotImplementedException("Specifier type not implemented for DATETRUNC");
	}
}

struct DateTruncBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA specifier, TB date) {
		return TruncateElement<TB, TR>(GetDatePartSpecifier(specifier.GetString()), date);
	}
};

template <typename TA, typename TR>
static void DateTruncUnaryExecutor(DatePartSpecifier type, Vector &left, Vector &result, idx_t count) {
	switch (type) {
	case DatePartSpecifier::MILLENNIUM:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::MillenniumOperator>(left, result, count);
		break;
	case DatePartSpecifier::CENTURY:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::CenturyOperator>(left, result, count);
		break;
	case DatePartSpecifier::DECADE:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::DecadeOperator>(left, result, count);
		break;
	case DatePartSpecifier::YEAR:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::YearOperator>(left, result, count);
		break;
	case DatePartSpecifier::QUARTER:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::QuarterOperator>(left, result, count);
		break;
	case DatePartSpecifier::MONTH:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::MonthOperator>(left, result, count);
		break;
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::WeekOperator>(left, result, count);
		break;
	case DatePartSpecifier::ISOYEAR:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::ISOYearOperator>(left, result, count);
		break;
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::DayOperator>(left, result, count);
		break;
	case DatePartSpecifier::HOUR:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::HourOperator>(left, result, count);
		break;
	case DatePartSpecifier::MINUTE:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::MinuteOperator>(left, result, count);
		break;
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::SecondOperator>(left, result, count);
		break;
	case DatePartSpecifier::MILLISECONDS:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::MillisecondOperator>(left, result, count);
		break;
	case DatePartSpecifier::MICROSECONDS:
		DateTrunc::UnaryExecute<TA, TR, DateTrunc::MicrosecondOperator>(left, result, count);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented for DATETRUNC");
	}
}

template <typename TA, typename TR>
static void DateTruncFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);
	auto &part_arg = args.data[0];
	auto &date_arg = args.data[1];

	if (part_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// Common case of constant part.
		if (ConstantVector::IsNull(part_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			const auto type = GetDatePartSpecifier(ConstantVector::GetData<string_t>(part_arg)->GetString());
			DateTruncUnaryExecutor<TA, TR>(type, date_arg, result, args.size());
		}
	} else {
		BinaryExecutor::ExecuteStandard<string_t, TA, TR, DateTruncBinaryOperator>(part_arg, date_arg, result,
		                                                                           args.size());
	}
}

template <class TA, class TR, class OP>
static unique_ptr<BaseStatistics> DateTruncStatistics(vector<BaseStatistics> &child_stats) {
	// we can only propagate date stats if the child has stats
	auto &nstats = child_stats[1];
	if (!NumericStats::HasMinMax(nstats)) {
		return nullptr;
	}
	// run the operator on both the min and the max, this gives us the [min, max] bound
	auto min = NumericStats::GetMin<TA>(nstats);
	auto max = NumericStats::GetMax<TA>(nstats);
	if (min > max) {
		return nullptr;
	}

	// Infinite values are unmodified
	auto min_part = DateTrunc::UnaryFunction<TA, TR, OP>(min);
	auto max_part = DateTrunc::UnaryFunction<TA, TR, OP>(max);

	auto min_value = Value::CreateValue(min_part);
	auto max_value = Value::CreateValue(max_part);
	auto result = NumericStats::CreateEmpty(min_value.type());
	NumericStats::SetMin(result, min_value);
	NumericStats::SetMax(result, max_value);
	result.CopyValidity(child_stats[0]);
	return result.ToUnique();
}

template <class TA, class TR, class OP>
static unique_ptr<BaseStatistics> PropagateDateTruncStatistics(ClientContext &context, FunctionStatisticsInput &input) {
	return DateTruncStatistics<TA, TR, OP>(input.child_stats);
}

template <typename TA, typename TR>
static function_statistics_t DateTruncStats(DatePartSpecifier type) {
	switch (type) {
	case DatePartSpecifier::MILLENNIUM:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::MillenniumOperator>;
	case DatePartSpecifier::CENTURY:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::CenturyOperator>;
	case DatePartSpecifier::DECADE:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::DecadeOperator>;
	case DatePartSpecifier::YEAR:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::YearOperator>;
	case DatePartSpecifier::QUARTER:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::QuarterOperator>;
	case DatePartSpecifier::MONTH:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::MonthOperator>;
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::WeekOperator>;
	case DatePartSpecifier::ISOYEAR:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::ISOYearOperator>;
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::DayOperator>;
	case DatePartSpecifier::HOUR:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::HourOperator>;
	case DatePartSpecifier::MINUTE:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::MinuteOperator>;
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::SecondOperator>;
	case DatePartSpecifier::MILLISECONDS:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::MillisecondOperator>;
	case DatePartSpecifier::MICROSECONDS:
		return PropagateDateTruncStatistics<TA, TR, DateTrunc::MicrosecondOperator>;
	default:
		throw NotImplementedException("Specifier type not implemented for DATETRUNC statistics");
	}
}

static unique_ptr<FunctionData> DateTruncBind(ClientContext &context, ScalarFunction &bound_function,
                                              vector<unique_ptr<Expression>> &arguments) {
	if (!arguments[0]->IsFoldable()) {
		return nullptr;
	}

	// Rebind to return a date if we are truncating that far
	Value part_value = ExpressionExecutor::EvaluateScalar(context, *arguments[0]);
	if (part_value.IsNull()) {
		return nullptr;
	}
	const auto part_name = part_value.ToString();
	const auto part_code = GetDatePartSpecifier(part_name);
	switch (part_code) {
	case DatePartSpecifier::MILLENNIUM:
	case DatePartSpecifier::CENTURY:
	case DatePartSpecifier::DECADE:
	case DatePartSpecifier::YEAR:
	case DatePartSpecifier::QUARTER:
	case DatePartSpecifier::MONTH:
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
	case DatePartSpecifier::ISOYEAR:
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		switch (bound_function.arguments[1].id()) {
		case LogicalType::TIMESTAMP:
			bound_function.function = DateTruncFunction<timestamp_t, date_t>;
			bound_function.statistics = DateTruncStats<timestamp_t, date_t>(part_code);
			break;
		case LogicalType::DATE:
			bound_function.function = DateTruncFunction<date_t, date_t>;
			bound_function.statistics = DateTruncStats<date_t, date_t>(part_code);
			break;
		default:
			throw NotImplementedException("Temporal argument type for DATETRUNC");
		}
		bound_function.return_type = LogicalType::DATE;
		break;
	default:
		switch (bound_function.arguments[1].id()) {
		case LogicalType::TIMESTAMP:
			bound_function.statistics = DateTruncStats<timestamp_t, timestamp_t>(part_code);
			break;
		case LogicalType::DATE:
			bound_function.statistics = DateTruncStats<date_t, timestamp_t>(part_code);
			break;
		default:
			throw NotImplementedException("Temporal argument type for DATETRUNC");
		}
		break;
	}

	return nullptr;
}

ScalarFunctionSet DateTruncFun::GetFunctions() {
	ScalarFunctionSet date_trunc("date_trunc");
	date_trunc.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, LogicalType::TIMESTAMP,
	                                      DateTruncFunction<timestamp_t, timestamp_t>, DateTruncBind));
	date_trunc.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::DATE}, LogicalType::TIMESTAMP,
	                                      DateTruncFunction<date_t, timestamp_t>, DateTruncBind));
	date_trunc.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::INTERVAL}, LogicalType::INTERVAL,
	                                      DateTruncFunction<interval_t, interval_t>));
	return date_trunc;
}

} // namespace duckdb
