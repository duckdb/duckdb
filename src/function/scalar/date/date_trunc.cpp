#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/string_util.hpp"

// TODO date_trunc function should also handle interval data type when it is implemented. See
// https://www.postgresql.org/docs/9.1/functions-datetime.html

namespace duckdb {

struct DateTrunc {

	struct MillenniumOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			date_t date = Timestamp::GetDate(input);
			return Timestamp::FromDatetime(Date::FromDate((Date::ExtractYear(date) / 1000) * 1000, 1, 1), dtime_t(0));
		}
	};

	struct CenturyOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			date_t date = Timestamp::GetDate(input);
			return Timestamp::FromDatetime(Date::FromDate((Date::ExtractYear(date) / 100) * 100, 1, 1), dtime_t(0));
		}
	};

	struct DecadeOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			date_t date = Timestamp::GetDate(input);
			return Timestamp::FromDatetime(Date::FromDate((Date::ExtractYear(date) / 10) * 10, 1, 1), dtime_t(0));
		}
	};

	struct YearOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			date_t date = Timestamp::GetDate(input);
			return Timestamp::FromDatetime(Date::FromDate(Date::ExtractYear(date), 1, 1), dtime_t(0));
		}
	};

	struct QuarterOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			int32_t yyyy, mm, dd;
			Date::Convert(Timestamp::GetDate(input), yyyy, mm, dd);
			mm = 1 + (((mm - 1) / 3) * 3);
			return Timestamp::FromDatetime(Date::FromDate(yyyy, mm, 1), dtime_t(0));
		}
	};

	struct MonthOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			date_t date = Timestamp::GetDate(input);
			return Timestamp::FromDatetime(Date::FromDate(Date::ExtractYear(date), Date::ExtractMonth(date), 1),
			                               dtime_t(0));
		}
	};

	struct WeekOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			date_t date = Timestamp::GetDate(input);

			return Timestamp::FromDatetime(Date::GetMondayOfCurrentWeek(date), dtime_t(0));
		}
	};

	struct ISOYearOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			date_t date = Date::GetMondayOfCurrentWeek(input);
			date.days -= (Date::ExtractISOWeekNumber(date) - 1) * Interval::DAYS_PER_WEEK;

			return Timestamp::FromDatetime(date, dtime_t(0));
		}
	};

	struct DayOperator {
		template <class TA, class TR>
		static inline TR Operation(TA input) {
			date_t date = Timestamp::GetDate(input);
			return Timestamp::FromDatetime(date, dtime_t(0));
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
timestamp_t DateTrunc::MillenniumOperator::Operation(date_t input) {
	return MillenniumOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, dtime_t(0)));
}

template <>
timestamp_t DateTrunc::CenturyOperator::Operation(date_t input) {
	return CenturyOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, dtime_t(0)));
}

template <>
timestamp_t DateTrunc::DecadeOperator::Operation(date_t input) {
	return DecadeOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, dtime_t(0)));
}

template <>
timestamp_t DateTrunc::YearOperator::Operation(date_t input) {
	return YearOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, dtime_t(0)));
}

template <>
timestamp_t DateTrunc::QuarterOperator::Operation(date_t input) {
	return QuarterOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, dtime_t(0)));
}

template <>
timestamp_t DateTrunc::MonthOperator::Operation(date_t input) {
	return MonthOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, dtime_t(0)));
}

template <>
timestamp_t DateTrunc::WeekOperator::Operation(date_t input) {
	return WeekOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, dtime_t(0)));
}

template <>
timestamp_t DateTrunc::ISOYearOperator::Operation(timestamp_t input) {
	return ISOYearOperator::Operation<date_t, timestamp_t>(Timestamp::GetDate(input));
}

template <>
timestamp_t DateTrunc::DayOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(input, dtime_t(0));
}

template <>
timestamp_t DateTrunc::HourOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
timestamp_t DateTrunc::MinuteOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
timestamp_t DateTrunc::SecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
timestamp_t DateTrunc::MillisecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <>
timestamp_t DateTrunc::MicrosecondOperator::Operation(date_t input) {
	return DayOperator::Operation<date_t, timestamp_t>(input);
}

template <class TA, class TR>
static TR TruncateElement(DatePartSpecifier type, TA element) {
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
		UnaryExecutor::Execute<TA, TR, DateTrunc::MillenniumOperator>(left, result, count);
		break;
	case DatePartSpecifier::CENTURY:
		UnaryExecutor::Execute<TA, TR, DateTrunc::CenturyOperator>(left, result, count);
		break;
	case DatePartSpecifier::DECADE:
		UnaryExecutor::Execute<TA, TR, DateTrunc::DecadeOperator>(left, result, count);
		break;
	case DatePartSpecifier::YEAR:
		UnaryExecutor::Execute<TA, TR, DateTrunc::YearOperator>(left, result, count);
		break;
	case DatePartSpecifier::QUARTER:
		UnaryExecutor::Execute<TA, TR, DateTrunc::QuarterOperator>(left, result, count);
		break;
	case DatePartSpecifier::MONTH:
		UnaryExecutor::Execute<TA, TR, DateTrunc::MonthOperator>(left, result, count);
		break;
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		UnaryExecutor::Execute<TA, TR, DateTrunc::WeekOperator>(left, result, count);
		break;
	case DatePartSpecifier::ISOYEAR:
		UnaryExecutor::Execute<TA, TR, DateTrunc::ISOYearOperator>(left, result, count);
		break;
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
		UnaryExecutor::Execute<TA, TR, DateTrunc::DayOperator>(left, result, count);
		break;
	case DatePartSpecifier::HOUR:
		UnaryExecutor::Execute<TA, TR, DateTrunc::HourOperator>(left, result, count);
		break;
	case DatePartSpecifier::MINUTE:
		UnaryExecutor::Execute<TA, TR, DateTrunc::MinuteOperator>(left, result, count);
		break;
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		UnaryExecutor::Execute<TA, TR, DateTrunc::SecondOperator>(left, result, count);
		break;
	case DatePartSpecifier::MILLISECONDS:
		UnaryExecutor::Execute<TA, TR, DateTrunc::MillisecondOperator>(left, result, count);
		break;
	case DatePartSpecifier::MICROSECONDS:
		UnaryExecutor::Execute<TA, TR, DateTrunc::MicrosecondOperator>(left, result, count);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented for DATETRUNC");
	}
}

template <typename T>
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
			DateTruncUnaryExecutor<T, timestamp_t>(type, date_arg, result, args.size());
		}
	} else {
		BinaryExecutor::ExecuteStandard<string_t, T, timestamp_t, DateTruncBinaryOperator>(part_arg, date_arg, result,
		                                                                                   args.size());
	}
}

void DateTruncFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet date_trunc("date_trunc");
	date_trunc.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP}, LogicalType::TIMESTAMP,
	                                      DateTruncFunction<timestamp_t>));
	date_trunc.AddFunction(
	    ScalarFunction({LogicalType::VARCHAR, LogicalType::DATE}, LogicalType::TIMESTAMP, DateTruncFunction<date_t>));
	set.AddFunction(date_trunc);
	date_trunc.name = "datetrunc";
	set.AddFunction(date_trunc);
}

} // namespace duckdb
