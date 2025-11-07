#include "core_functions/scalar/date_functions.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

namespace {

struct DateSub {
	static int64_t SubtractMicros(timestamp_t startdate, timestamp_t enddate) {
		const auto start = Timestamp::GetEpochMicroSeconds(startdate);
		const auto end = Timestamp::GetEpochMicroSeconds(enddate);
		return SubtractOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(end, start);
	}

	template <class TA, class TB, class TR, class OP>
	static inline void BinaryExecute(Vector &left, Vector &right, Vector &result, idx_t count) {
		BinaryExecutor::ExecuteWithNulls<TA, TB, TR>(
		    left, right, result, count, [&](TA startdate, TB enddate, ValidityMask &mask, idx_t idx) {
			    if (Value::IsFinite(startdate) && Value::IsFinite(enddate)) {
				    return OP::template Operation<TA, TB, TR>(startdate, enddate);
			    } else {
				    mask.SetInvalid(idx);
				    return TR();
			    }
		    });
	}

	struct MonthOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA start_ts, TB end_ts) {
			if (start_ts > end_ts) {
				return -MonthOperator::Operation<TA, TB, TR>(end_ts, start_ts);
			}
			// The number of complete months depends on whether end_ts is on the last day of the month.
			date_t end_date;
			dtime_t end_time;
			Timestamp::Convert(end_ts, end_date, end_time);

			int32_t yyyy, mm, dd;
			Date::Convert(end_date, yyyy, mm, dd);
			const auto end_days = Date::MonthDays(yyyy, mm);
			if (end_days == dd) {
				// Now check whether the start day is after the end day
				date_t start_date;
				dtime_t start_time;
				Timestamp::Convert(start_ts, start_date, start_time);
				Date::Convert(start_date, yyyy, mm, dd);
				if (dd > end_days || (dd == end_days && start_time < end_time)) {
					// Move back to the same time on the last day of the (shorter) end month
					start_date = Date::FromDate(yyyy, mm, end_days);
					start_ts = Timestamp::FromDatetime(start_date, start_time);
				}
			}

			// Our interval difference will now give the correct result.
			// Note that PG gives different interval subtraction results,
			// so if we change this we will have to reimplement.
			return Interval::GetAge(end_ts, start_ts).months;
		}
	};

	struct QuarterOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA start_ts, TB end_ts) {
			return MonthOperator::Operation<TA, TB, TR>(start_ts, end_ts) / Interval::MONTHS_PER_QUARTER;
		}
	};

	struct YearOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA start_ts, TB end_ts) {
			return MonthOperator::Operation<TA, TB, TR>(start_ts, end_ts) / Interval::MONTHS_PER_YEAR;
		}
	};

	struct DecadeOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA start_ts, TB end_ts) {
			return MonthOperator::Operation<TA, TB, TR>(start_ts, end_ts) / Interval::MONTHS_PER_DECADE;
		}
	};

	struct CenturyOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA start_ts, TB end_ts) {
			return MonthOperator::Operation<TA, TB, TR>(start_ts, end_ts) / Interval::MONTHS_PER_CENTURY;
		}
	};

	struct MilleniumOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA start_ts, TB end_ts) {
			return MonthOperator::Operation<TA, TB, TR>(start_ts, end_ts) / Interval::MONTHS_PER_MILLENIUM;
		}
	};

	struct DayOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate) / Interval::MICROS_PER_DAY;
		}
	};

	struct WeekOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate) / Interval::MICROS_PER_WEEK;
		}
	};

	struct MicrosecondsOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate);
		}
	};

	struct MillisecondsOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate) / Interval::MICROS_PER_MSEC;
		}
	};

	struct SecondsOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate) / Interval::MICROS_PER_SEC;
		}
	};

	struct MinutesOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate) / Interval::MICROS_PER_MINUTE;
		}
	};

	struct HoursOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA startdate, TB enddate) {
			return SubtractMicros(startdate, enddate) / Interval::MICROS_PER_HOUR;
		}
	};
};

// DATE specialisations
template <>
int64_t DateSub::YearOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return YearOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                  Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::MonthOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return MonthOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                   Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::DayOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return DayOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                 Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::DecadeOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return DecadeOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                    Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::CenturyOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return CenturyOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                     Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::MilleniumOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return MilleniumOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                       Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::QuarterOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return QuarterOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                     Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::WeekOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return WeekOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                  Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::MicrosecondsOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return MicrosecondsOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                          Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::MillisecondsOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return MillisecondsOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                          Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::SecondsOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return SecondsOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                     Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::MinutesOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return MinutesOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                     Timestamp::FromDatetime(enddate, t0));
}

template <>
int64_t DateSub::HoursOperator::Operation(date_t startdate, date_t enddate) {
	dtime_t t0(0);
	return HoursOperator::Operation<timestamp_t, timestamp_t, int64_t>(Timestamp::FromDatetime(startdate, t0),
	                                                                   Timestamp::FromDatetime(enddate, t0));
}

// TIME specialisations
template <>
int64_t DateSub::YearOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"year\" not recognized");
}

template <>
int64_t DateSub::MonthOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"month\" not recognized");
}

template <>
int64_t DateSub::DayOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"day\" not recognized");
}

template <>
int64_t DateSub::DecadeOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"decade\" not recognized");
}

template <>
int64_t DateSub::CenturyOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"century\" not recognized");
}

template <>
int64_t DateSub::MilleniumOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"millennium\" not recognized");
}

template <>
int64_t DateSub::QuarterOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"quarter\" not recognized");
}

template <>
int64_t DateSub::WeekOperator::Operation(dtime_t startdate, dtime_t enddate) {
	throw NotImplementedException("\"time\" units \"week\" not recognized");
}

template <>
int64_t DateSub::MicrosecondsOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return enddate.micros - startdate.micros;
}

template <>
int64_t DateSub::MillisecondsOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return (enddate.micros - startdate.micros) / Interval::MICROS_PER_MSEC;
}

template <>
int64_t DateSub::SecondsOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return (enddate.micros - startdate.micros) / Interval::MICROS_PER_SEC;
}

template <>
int64_t DateSub::MinutesOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return (enddate.micros - startdate.micros) / Interval::MICROS_PER_MINUTE;
}

template <>
int64_t DateSub::HoursOperator::Operation(dtime_t startdate, dtime_t enddate) {
	return (enddate.micros - startdate.micros) / Interval::MICROS_PER_HOUR;
}

template <typename TA, typename TB, typename TR>
int64_t SubtractDateParts(DatePartSpecifier type, TA startdate, TB enddate) {
	switch (type) {
	case DatePartSpecifier::YEAR:
	case DatePartSpecifier::ISOYEAR:
		return DateSub::YearOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MONTH:
		return DateSub::MonthOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		return DateSub::DayOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::DECADE:
		return DateSub::DecadeOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::CENTURY:
		return DateSub::CenturyOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MILLENNIUM:
		return DateSub::MilleniumOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::QUARTER:
		return DateSub::QuarterOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		return DateSub::WeekOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MICROSECONDS:
		return DateSub::MicrosecondsOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MILLISECONDS:
		return DateSub::MillisecondsOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		return DateSub::SecondsOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::MINUTE:
		return DateSub::MinutesOperator::template Operation<TA, TB, TR>(startdate, enddate);
	case DatePartSpecifier::HOUR:
		return DateSub::HoursOperator::template Operation<TA, TB, TR>(startdate, enddate);
	default:
		throw NotImplementedException("Specifier type not implemented for DATESUB");
	}
}

struct DateSubTernaryOperator {
	template <typename TS, typename TA, typename TB, typename TR>
	static inline TR Operation(TS part, TA startdate, TB enddate, ValidityMask &mask, idx_t idx) {
		if (Value::IsFinite(startdate) && Value::IsFinite(enddate)) {
			return SubtractDateParts<TA, TB, TR>(GetDatePartSpecifier(part.GetString()), startdate, enddate);
		} else {
			mask.SetInvalid(idx);
			return TR();
		}
	}
};

template <typename TA, typename TB, typename TR>
void DateSubBinaryExecutor(DatePartSpecifier type, Vector &left, Vector &right, Vector &result, idx_t count) {
	switch (type) {
	case DatePartSpecifier::YEAR:
	case DatePartSpecifier::ISOYEAR:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::YearOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MONTH:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::MonthOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::DayOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::DECADE:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::DecadeOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::CENTURY:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::CenturyOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MILLENNIUM:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::MilleniumOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::QUARTER:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::QuarterOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::WeekOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MICROSECONDS:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::MicrosecondsOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MILLISECONDS:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::MillisecondsOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::SecondsOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::MINUTE:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::MinutesOperator>(left, right, result, count);
		break;
	case DatePartSpecifier::HOUR:
		DateSub::BinaryExecute<TA, TB, TR, DateSub::HoursOperator>(left, right, result, count);
		break;
	default:
		throw NotImplementedException("Specifier type not implemented for DATESUB");
	}
}

template <typename T>
void DateSubFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);
	auto &part_arg = args.data[0];
	auto &start_arg = args.data[1];
	auto &end_arg = args.data[2];

	if (part_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		// Common case of constant part.
		if (ConstantVector::IsNull(part_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			const auto type = GetDatePartSpecifier(ConstantVector::GetData<string_t>(part_arg)->GetString());
			DateSubBinaryExecutor<T, T, int64_t>(type, start_arg, end_arg, result, args.size());
		}
	} else {
		TernaryExecutor::ExecuteWithNulls<string_t, T, T, int64_t>(
		    part_arg, start_arg, end_arg, result, args.size(),
		    DateSubTernaryOperator::Operation<string_t, T, T, int64_t>);
	}
}

} // namespace

ScalarFunctionSet DateSubFun::GetFunctions() {
	ScalarFunctionSet date_sub("date_sub");
	date_sub.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::DATE, LogicalType::DATE},
	                                    LogicalType::BIGINT, DateSubFunction<date_t>));
	date_sub.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP},
	                                    LogicalType::BIGINT, DateSubFunction<timestamp_t>));
	date_sub.AddFunction(ScalarFunction({LogicalType::VARCHAR, LogicalType::TIME, LogicalType::TIME},
	                                    LogicalType::BIGINT, DateSubFunction<dtime_t>));
	return date_sub;
}

} // namespace duckdb
