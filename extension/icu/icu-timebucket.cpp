#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "include/icu-datefunc.hpp"
#include "include/icu-timezone.hpp"

namespace duckdb {

struct ICUFromLocalTime : public ICUDateFunc {
	static inline timestamp_t Operation(icu::Calendar *calendar, timestamp_t local) {
		// Extract the parts from the "instant"
		date_t local_date;
		dtime_t local_time;
		Timestamp::Convert(local, local_date, local_time);

		int32_t year;
		int32_t mm;
		int32_t dd;
		Date::Convert(local_date, year, mm, dd);

		int32_t hr;
		int32_t mn;
		int32_t secs;
		int32_t frac;
		Time::Convert(local_time, hr, mn, secs, frac);
		int32_t millis = frac / Interval::MICROS_PER_MSEC;
		uint64_t micros = frac % Interval::MICROS_PER_MSEC;

		// Use them to set the time in the time zone
		calendar->set(UCAL_YEAR, year);
		calendar->set(UCAL_MONTH, int32_t(mm - 1));
		calendar->set(UCAL_DATE, dd);
		calendar->set(UCAL_HOUR_OF_DAY, hr);
		calendar->set(UCAL_MINUTE, mn);
		calendar->set(UCAL_SECOND, secs);
		calendar->set(UCAL_MILLISECOND, millis);

		return GetTime(calendar, micros);
	}
};

struct ICUToLocalTime : public ICUDateFunc {
	static inline timestamp_t Operation(icu::Calendar *calendar, timestamp_t instant) {
		// Extract the time zone parts
		auto micros = SetTime(calendar, instant);
		const auto year = ExtractField(calendar, UCAL_YEAR);
		const auto mm = ExtractField(calendar, UCAL_MONTH) + 1;
		const auto dd = ExtractField(calendar, UCAL_DATE);

		date_t local_date;
		if (!Date::TryFromDate(year, mm, dd, local_date)) {
			throw ConversionException("Unable to create local date in TIMEZONE function");
		}

		const auto hr = ExtractField(calendar, UCAL_HOUR_OF_DAY);
		const auto mn = ExtractField(calendar, UCAL_MINUTE);
		const auto secs = ExtractField(calendar, UCAL_SECOND);
		const auto millis = ExtractField(calendar, UCAL_MILLISECOND);

		micros += millis * Interval::MICROS_PER_MSEC;
		dtime_t local_time = Time::FromTime(hr, mn, secs, micros);

		timestamp_t result;
		if (!Timestamp::TryFromDatetime(local_date, local_time, result)) {
			throw ConversionException("Unable to create local timestamp in TIMEZONE function");
		}

		return result;
	}
};

struct ICUTimeBucket : public ICUDateFunc {

	constexpr static const int64_t DEFAULT_ORIGIN_DAYS = 10959;
	constexpr static const int32_t DEFAULT_ORIGIN_MONTHS = 360;

	static int32_t EpochMonths(timestamp_t ts) {
		date_t ts_date = Cast::template Operation<timestamp_t, date_t>(ts);
		return (Date::ExtractYear(ts_date) - 1970) * 12 + Date::ExtractMonth(ts_date) - 1;
	}

	static int32_t ExtractYearFromEpochMonths(int32_t epoch_months) {
		return (epoch_months < 0 && epoch_months % 12 != 0) ? 1970 + epoch_months / 12 - 1 : 1970 + epoch_months / 12;
	}

	static int32_t ExtractMonthFromEpochMonths(int32_t epoch_months) {
		return (epoch_months < 0 && epoch_months % 12 != 0) ? epoch_months % 12 + 13 : epoch_months % 12 + 1;
	}

	static int64_t WidthLessThanDaysCommon(int64_t bucket_width_micros, int64_t ts_micros, int64_t origin_micros) {
		origin_micros %= bucket_width_micros;
		if (origin_micros > 0 && ts_micros < NumericLimits<int64_t>::Minimum() + origin_micros) {
			throw OutOfRangeException("Timestamp out of range");
		}
		if (origin_micros < 0 && ts_micros > NumericLimits<int64_t>::Maximum() + origin_micros) {
			throw OutOfRangeException("Timestamp out of range");
		}
		ts_micros -= origin_micros;

		int64_t result_micros = (ts_micros / bucket_width_micros) * bucket_width_micros;
		ts_micros -= result_micros;
		if (ts_micros < 0 && ts_micros % bucket_width_micros != 0) {
			if (result_micros < NumericLimits<int64_t>::Minimum() + bucket_width_micros) {
				throw OutOfRangeException("Timestamp out of range");
			}
			result_micros -= bucket_width_micros;
		}
		result_micros += origin_micros;

		return result_micros;
	}

	static int32_t WidthMoreThanMonthsCommon(int32_t bucket_width_months, int32_t ts_months, int32_t origin_months) {
		origin_months %= bucket_width_months;
		if (origin_months > 0 && ts_months < NumericLimits<int32_t>::Minimum() + origin_months) {
			throw NotImplementedException("Timestamp out of range");
		}
		if (origin_months < 0 && ts_months > NumericLimits<int32_t>::Maximum() + origin_months) {
			throw NotImplementedException("Timestamp out of range");
		}
		ts_months -= origin_months;

		int32_t result_months = (ts_months / bucket_width_months) * bucket_width_months;
		if (ts_months < 0 && ts_months % bucket_width_months != 0) {
			if (result_months < NumericLimits<int32_t>::Minimum() + bucket_width_months) {
				throw OutOfRangeException("Timestamp out of range");
			}
			result_months = result_months - bucket_width_months;
		}
		result_months += origin_months;

		return result_months;
	}

	template <typename TA, typename TB, typename TR, typename OP>
	static void ExecuteBinary(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		BinaryExecutor::Execute<TA, TB, TR>(args.data[0], args.data[1], result, args.size(), [&](TA left, TB right) {
			return OP::template Operation<TA, TB, TR>(left, right, calendar);
		});
	}

	template <typename TA, typename TB, typename TC, typename TR, typename OP>
	static void ExecuteTernary(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 3);

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		TernaryExecutor::Execute<TA, TB, TC, TR>(
		    args.data[0], args.data[1], args.data[2], result, args.size(), [&](TA ta, TB tb, TC tc) {
			    return OP::template Operation<TA, TB, TC, TR>(args.data[0], args.data[1], args.data[2], calendar.get());
		    });
	}

	struct WidthLessThanDaysBinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts, icu::Calendar *calendar) {
			throw InternalException("Unimplemented type for WidthLessThanDaysBinaryOperator");
		}
	};

	struct WidthMoreThanMonthsBinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts, icu::Calendar *calendar) {
			throw InternalException("Unimplemented type for WidthMoreThanMonthsBinaryOperator");
		}
	};

	struct BinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts, icu::Calendar *calendar) {
			throw InternalException("Unimplemented type for WidthMoreThanMonthsBinaryOperator");
		}
	};

	struct OffsetWidthLessThanDaysTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset, icu::Calendar *calendar) {
			throw InternalException("Unimplemented type for OffsetWidthLessThanDaysTernaryOperator");
		}
	};

	struct OffsetWidthMoreThanMonthsTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset, icu::Calendar *calendar) {
			throw InternalException("Unimplemented type for OffsetWidthMoreThanMonthsTernaryOperator");
		}
	};

	struct OffsetTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset, icu::Calendar *calendar) {
			throw InternalException("Unimplemented type for OffsetTernaryOperator");
		}
	};

	struct OriginWidthLessThanDaysTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC origin, icu::Calendar *calendar) {
			throw InternalException("Unimplemented type for OriginWidthLessThanDaysTernaryOperator");
		}
	};

	struct OriginWidthMoreThanMonthsTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC origin, icu::Calendar *calendar) {
			throw InternalException("Unimplemented type for OriginWidthMoreThanMonthsTernaryOperator");
		}
	};

	struct OriginTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC origin, ValidityMask &mask, idx_t idx,
		                           icu::Calendar *calendar) {
			throw InternalException("Unimplemented type for OriginTernaryOperator");
		}
	};

	struct TimeZoneWidthLessThanDaysBinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts, icu::Calendar *calendar) {
			throw InternalException("Unimplemented type for TimeZoneWidthLessThanDaysTernaryOperator");
		}
	};

	struct TimeZoneWidthMoreThanMonthsBinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts, icu::Calendar *calendar) {
			throw InternalException("Unimplemented type for TimeZoneWidthMoreThanMonthsTernaryOperator");
		}
	};

	struct TimeZoneTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC tz,
		                           icu::Calendar *calendar) {
			throw InternalException("Unimplemented type for TimeZoneTernaryOperator");
		}
	};

	static void ICUTimeBucketFunction(DataChunk &args, ExpressionState &state, Vector &result);

	static void ICUTimeBucketOffsetFunction(DataChunk &args, ExpressionState &state, Vector &result);

	static void ICUTimeBucketOriginFunction(DataChunk &args, ExpressionState &state, Vector &result);

	static void ICUTimeBucketTimeZoneFunction(DataChunk &args, ExpressionState &state, Vector &result);

	static void AddTimeBucketFunction(const string &name, ClientContext &context);
};

template <>
timestamp_t ICUTimeBucket::WidthLessThanDaysBinaryOperator::Operation(interval_t bucket_width,
                                                                                             timestamp_t ts,
                                                                                             icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
	int64_t ts_micros = Timestamp::GetEpochMicroSeconds(ICUToLocalTime::Operation(calendar, ts));
	timestamp_t origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_DAYS * Interval::MICROS_PER_DAY);
	// set origin 2000-01-03 00:00:00 (Monday) for TimescaleDB compatibility
	// there are 10959 days between 1970-01-01 00:00:00 and 2000-01-03 00:00:00
	int64_t origin_micros = DEFAULT_ORIGIN_DAYS * Interval::MICROS_PER_DAY;

	return ICUFromLocalTime::Operation(calendar, Timestamp::FromEpochMicroSeconds(WidthLessThanDaysCommon(bucket_width_micros, ts_micros, origin_micros)));
}

template <>
timestamp_t ICUTimeBucket::WidthMoreThanMonthsBinaryOperator::Operation(
    interval_t bucket_width, timestamp_t ts, icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	int32_t ts_months = EpochMonths(ICUToLocalTime::Operation(calendar, ts));
	// set origin 2000-01-01 00:00:00 for TimescaleDB compatibility
	// there are 360 months between 1970-01-01 00:00:00 and 2000-01-01 00:00:00
	int32_t origin_months = DEFAULT_ORIGIN_MONTHS;

	int32_t result_months = WidthMoreThanMonthsCommon(bucket_width.months, ts_months, origin_months);
	int32_t year = ExtractYearFromEpochMonths(result_months);
	int32_t month = ExtractMonthFromEpochMonths(result_months);
	return ICUFromLocalTime::Operation(calendar, Cast::template Operation<date_t, timestamp_t>(Date::FromDate(year, month, 1)));
}

template <>
timestamp_t ICUTimeBucket::BinaryOperator::Operation(interval_t bucket_width,
                                                                                           timestamp_t ts,
                                                                                           icu::Calendar *calendar) {
	if (bucket_width.months == 0) {
		int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
		if (bucket_width_micros <= 0) {
			throw NotImplementedException("Period must be greater than 0");
		}
		return WidthLessThanDaysBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(bucket_width, ts,
		                                                                                        calendar);
	} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
		if (bucket_width.months < 0) {
			throw NotImplementedException("Period must be greater than 0");
		}
		return WidthMoreThanMonthsBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(bucket_width, ts,
		                                                                                          calendar);
	} else {
		throw NotImplementedException("Month intervals cannot have day or time component");
	}
}

template <>
timestamp_t ICUTimeBucket::OffsetWidthLessThanDaysTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                             interval_t offset, icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
	int64_t ts_micros = Timestamp::GetEpochMicroSeconds(
	    Interval::Add(ICUToLocalTime::Operation(calendar, ts), Interval::Invert(offset)));
	// set origin 2000-01-03 00:00:00 (Monday) for TimescaleDB compatibility
	// there are 10959 days between 1970-01-01 00:00:00 and 2000-01-03 00:00:00
	int64_t origin_micros = DEFAULT_ORIGIN_DAYS * Interval::MICROS_PER_DAY;

	return ICUFromLocalTime::Operation(calendar, Interval::Add(Timestamp::FromEpochMicroSeconds(WidthLessThanDaysCommon(
	                                                               bucket_width_micros, ts_micros, origin_micros)),
	                                                           offset));
}

template <>
timestamp_t ICUTimeBucket::OffsetWidthMoreThanMonthsTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                             interval_t offset,
                                                                             icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	int32_t ts_months = EpochMonths(Interval::Add(ICUToLocalTime::Operation(calendar, ts), Interval::Invert(offset)));
	// set origin 2000-01-01 00:00:00 for TimescaleDB compatibility
	// there are 360 months between 1970-01-01 00:00:00 and 2000-01-01 00:00:00
	int32_t origin_months = DEFAULT_ORIGIN_MONTHS;

	int32_t result_months = WidthMoreThanMonthsCommon(bucket_width.months, ts_months, origin_months);
	int32_t year = ExtractYearFromEpochMonths(result_months);
	int32_t month = ExtractMonthFromEpochMonths(result_months);
	return ICUFromLocalTime::Operation(
	    calendar, Interval::Add(Cast::template Operation<date_t, timestamp_t>(Date::FromDate(year, month, 1)), offset));
}

template <>
timestamp_t ICUTimeBucket::OffsetTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                             interval_t offset,
                                                                             icu::Calendar *calendar) {
	if (bucket_width.months == 0) {
		int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
		if (bucket_width_micros <= 0) {
			throw NotImplementedException("Period must be greater than 0");
		}
		return OffsetWidthLessThanDaysTernaryOperator::Operation<interval_t, timestamp_t, interval_t, timestamp_t>(bucket_width, ts, offset, calendar);
	} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
		if (bucket_width.months < 0) {
			throw NotImplementedException("Period must be greater than 0");
		}
		return OffsetWidthMoreThanMonthsTernaryOperator::Operation<interval_t, timestamp_t, interval_t, timestamp_t>(
		    bucket_width, ts, offset, calendar);
	} else {
		throw NotImplementedException("Month intervals cannot have day or time component");
	}
}

template <>
timestamp_t ICUTimeBucket::OriginWidthLessThanDaysTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                             timestamp_t origin,
                                                                             icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
	int64_t ts_micros = Timestamp::GetEpochMicroSeconds(ICUToLocalTime::Operation(calendar, ts));
	int64_t origin_micros = Timestamp::GetEpochMicroSeconds(ICUToLocalTime::Operation(calendar, origin));

	return ICUFromLocalTime::Operation(
	    calendar, Timestamp::FromEpochMicroSeconds(WidthLessThanDaysCommon(bucket_width_micros, ts_micros, origin_micros)));
}

template <>
timestamp_t ICUTimeBucket::OriginWidthMoreThanMonthsTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                               timestamp_t origin,
                                                                               icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	int32_t ts_months = EpochMonths(ICUToLocalTime::Operation(calendar, ts));
	int32_t origin_months = EpochMonths(ICUToLocalTime::Operation(calendar, origin));

	int32_t result_months = WidthMoreThanMonthsCommon(bucket_width.months, ts_months, origin_months);
	int32_t year = ExtractYearFromEpochMonths(result_months);
	int32_t month = ExtractMonthFromEpochMonths(result_months);
	return ICUFromLocalTime::Operation(calendar,
	                                   Cast::template Operation<date_t, timestamp_t>(Date::FromDate(year, month, 1)));
}

template <>
timestamp_t ICUTimeBucket::OriginTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts, timestamp_t origin,
                                                            ValidityMask &mask, idx_t idx, icu::Calendar *calendar) {
	if (!Value::IsFinite(origin)) {
		mask.SetInvalid(idx);
		return timestamp_t(0);
	}
	if (bucket_width.months == 0) {
		int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
		if (bucket_width_micros <= 0) {
			throw NotImplementedException("Period must be greater than 0");
		}
		return OriginWidthLessThanDaysTernaryOperator::Operation<interval_t, timestamp_t, timestamp_t, timestamp_t>(bucket_width, ts, origin, calendar);
	} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
		if (bucket_width.months < 0) {
			throw NotImplementedException("Period must be greater than 0");
		}
		return OriginWidthMoreThanMonthsTernaryOperator::Operation<interval_t, timestamp_t, timestamp_t, timestamp_t>(bucket_width, ts, origin, calendar);
	} else {
		throw NotImplementedException("Month intervals cannot have day or time component");
	}
}

template <>
timestamp_t ICUTimeBucket::TimeZoneWidthLessThanDaysBinaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                             icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
	int64_t ts_micros = Timestamp::GetEpochMicroSeconds(ICUToLocalTime::Operation(calendar, ts));
	// set origin 2000-01-03 00:00:00 (Monday) for TimescaleDB compatibility
	// there are 10959 days between 1970-01-01 00:00:00 and 2000-01-03 00:00:00
	int64_t origin_micros = DEFAULT_ORIGIN_DAYS * Interval::MICROS_PER_DAY;

	return ICUFromLocalTime::Operation(calendar, Timestamp::FromEpochMicroSeconds(WidthLessThanDaysCommon(
	                                                 bucket_width_micros, ts_micros, origin_micros)));
}

template <>
timestamp_t ICUTimeBucket::TimeZoneWidthMoreThanMonthsBinaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                               icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	int32_t ts_months = EpochMonths(ICUToLocalTime::Operation(calendar, ts));
	// set origin 2000-01-01 00:00:00 for TimescaleDB compatibility
	// there are 360 months between 1970-01-01 00:00:00 and 2000-01-01 00:00:00
	int32_t origin_months = DEFAULT_ORIGIN_MONTHS;

	int32_t result_months = WidthMoreThanMonthsCommon(bucket_width.months, ts_months, origin_months);
	int32_t year = ExtractYearFromEpochMonths(result_months);
	int32_t month = ExtractMonthFromEpochMonths(result_months);
	return ICUFromLocalTime::Operation(calendar,
	                                   Cast::template Operation<date_t, timestamp_t>(Date::FromDate(year, month, 1)));
}

template <>
timestamp_t ICUTimeBucket::TimeZoneTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts, string_t tz,
                                                            icu::Calendar *calendar) {
	SetTimeZone(calendar, tz);
	if (bucket_width.months == 0) {
		int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
		if (bucket_width_micros <= 0) {
			throw NotImplementedException("Period must be greater than 0");
		}
		return TimeZoneWidthLessThanDaysBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(
		    bucket_width, ts, calendar);
	} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
		if (bucket_width.months < 0) {
			throw NotImplementedException("Period must be greater than 0");
		}
		return TimeZoneWidthMoreThanMonthsBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(
		    bucket_width, ts, calendar);
	} else {
		throw NotImplementedException("Month intervals cannot have day or time component");
	}
}

void ICUTimeBucket::ICUTimeBucketFunction(DataChunk &args, ExpressionState &state,
                                                                                Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (BindData &)*func_expr.bind_info;
	CalendarPtr calendar_ptr(info.calendar->clone());
	auto calendar = calendar_ptr.get();
	SetTimeZone(calendar, string_t("UTC"));

	auto &bucket_width_arg = args.data[0];
	auto &ts_arg = args.data[1];

	if (bucket_width_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(bucket_width_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			interval_t bucket_width = *ConstantVector::GetData<interval_t>(bucket_width_arg);
			if (bucket_width.months == 0) {
				int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
				if (bucket_width_micros <= 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
					    return WidthLessThanDaysBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(
					        bucket_width, ts, calendar);
				    });
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
					    return WidthMoreThanMonthsBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(
					        bucket_width, ts, calendar);
				    });
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
			}
		}
	} else {
		BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
		    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
			    return BinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(bucket_width, ts, calendar);
		    });
	}
}

void ICUTimeBucket::ICUTimeBucketOffsetFunction(DataChunk &args,
                                                                                      ExpressionState &state,
                                                                                      Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (BindData &)*func_expr.bind_info;
	CalendarPtr calendar_ptr(info.calendar->clone());
	auto calendar = calendar_ptr.get();
	SetTimeZone(calendar, string_t("UTC"));

	auto &bucket_width_arg = args.data[0];
	auto &ts_arg = args.data[1];
	auto &offset_arg = args.data[2];

	if (bucket_width_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(bucket_width_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			interval_t bucket_width = *ConstantVector::GetData<interval_t>(bucket_width_arg);
			if (bucket_width.months == 0) {
				int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
				if (bucket_width_micros <= 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
					    return OffsetWidthLessThanDaysTernaryOperator::Operation<interval_t, timestamp_t, interval_t, timestamp_t>(
					        bucket_width, ts, offset, calendar);
				    });
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
					    return OffsetWidthMoreThanMonthsTernaryOperator::Operation<interval_t, timestamp_t, interval_t, timestamp_t>(
					        bucket_width, ts, offset, calendar);
				    });
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
			}
		}
	} else {
		TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
		    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
		    [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
			    return OffsetTernaryOperator::Operation<interval_t, timestamp_t, interval_t, timestamp_t>(
			        bucket_width, ts, offset, calendar);
		    });
	}
}

void ICUTimeBucket::ICUTimeBucketOriginFunction(DataChunk &args,
                                                                                      ExpressionState &state,
                                                                                      Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (BindData &)*func_expr.bind_info;
	CalendarPtr calendar_ptr(info.calendar->clone());
	auto calendar = calendar_ptr.get();
	SetTimeZone(calendar, string_t("UTC"));

	auto &bucket_width_arg = args.data[0];
	auto &ts_arg = args.data[1];
	auto &origin_arg = args.data[2];

	if (bucket_width_arg.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	    origin_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(bucket_width_arg) || ConstantVector::IsNull(origin_arg) ||
		    !Value::IsFinite(*ConstantVector::GetData<timestamp_t>(origin_arg))) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			interval_t bucket_width = *ConstantVector::GetData<interval_t>(bucket_width_arg);
			if (bucket_width.months == 0) {
				int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
				if (bucket_width_micros <= 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				TernaryExecutor::Execute<interval_t, timestamp_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin) {
					    return OriginWidthLessThanDaysTernaryOperator::Operation<interval_t, timestamp_t, timestamp_t,
					                                                             timestamp_t>(bucket_width, ts, origin,
					                                                                          calendar);
				    });
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				TernaryExecutor::Execute<interval_t, timestamp_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin) {
					    return OriginWidthMoreThanMonthsTernaryOperator::Operation<interval_t, timestamp_t, timestamp_t,
					                                                               timestamp_t>(bucket_width, ts,
					                                                                            origin, calendar);
				    });
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
			}
		}
	} else {
		TernaryExecutor::ExecuteWithNulls<interval_t, timestamp_t, timestamp_t, timestamp_t>(
		    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
		    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin, ValidityMask &mask, idx_t idx) {
			    return OriginTernaryOperator::Operation<interval_t, timestamp_t, timestamp_t, timestamp_t>(
			        bucket_width, ts, origin, mask, idx, calendar);
		    });
	}
}

void ICUTimeBucket::ICUTimeBucketTimeZoneFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (BindData &)*func_expr.bind_info;
	CalendarPtr calendar_ptr(info.calendar->clone());
	auto calendar = calendar_ptr.get();

	auto &bucket_width_arg = args.data[0];
	auto &ts_arg = args.data[1];
	auto &tz_arg = args.data[2];

	if (bucket_width_arg.GetVectorType() == VectorType::CONSTANT_VECTOR && tz_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(bucket_width_arg) || ConstantVector::IsNull(tz_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			interval_t bucket_width = *ConstantVector::GetData<interval_t>(bucket_width_arg);
			SetTimeZone(calendar, *ConstantVector::GetData<string_t>(tz_arg));
			if (bucket_width.months == 0) {
				int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
				if (bucket_width_micros <= 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
					    return TimeZoneWidthLessThanDaysBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(
					        bucket_width, ts, calendar);
				    });
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
					    return TimeZoneWidthMoreThanMonthsBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(
					        bucket_width, ts, calendar);
				    });
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
			}
		}
	} else {
		TernaryExecutor::Execute<interval_t, timestamp_t, string_t, timestamp_t>(
		    bucket_width_arg, ts_arg, tz_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts, string_t tz) {
			    return TimeZoneTernaryOperator::Operation<interval_t, timestamp_t, string_t, timestamp_t>(bucket_width, ts, tz, calendar);
		    });
	}
}

void ICUTimeBucket::AddTimeBucketFunction(const string &name, ClientContext &context) {
	ScalarFunctionSet set("time_bucket");
	set.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP_TZ}, LogicalType::TIMESTAMP_TZ,
	                               ICUTimeBucketFunction, Bind));
	set.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP_TZ, LogicalType::INTERVAL}, LogicalType::TIMESTAMP_TZ,
	                               ICUTimeBucketOffsetFunction, Bind));
	set.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ},
	                               LogicalType::TIMESTAMP_TZ, ICUTimeBucketOriginFunction, Bind));
	set.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP_TZ, LogicalType::VARCHAR}, LogicalType::TIMESTAMP_TZ,
	                               ICUTimeBucketTimeZoneFunction, Bind));

	CreateScalarFunctionInfo func_info(set);
	auto &catalog = Catalog::GetCatalog(context);
	catalog.AddFunction(context, &func_info);
}

void RegisterICUTimeBucketFunctions(ClientContext &context) {
	ICUTimeBucket::AddTimeBucketFunction("time_bucket", context);
}

} // namespace duckdb
