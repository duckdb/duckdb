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
#include "include/icu-dateadd.hpp"
#include "include/icu-datefunc.hpp"
#include "include/icu-timezone.hpp"

namespace duckdb {

struct ICUTimeBucket : public ICUDateFunc {

	// Use 2000-01-03 00:00:00 (Monday) as origin when bucket_width is days, hours, ... for TimescaleDB compatibility
	//  There are 10959 days between 1970-01-01 and 2000-01-03
	constexpr static const int64_t DEFAULT_ORIGIN_MICROS_1 = 10959 * Interval::MICROS_PER_DAY;
	// Use 2000-01-01 as origin when bucket_width is months, years, ... for TimescaleDB compatibility
	// There are 10957 days between 1970-01-01 and 2000-01-01
	constexpr static const int64_t DEFAULT_ORIGIN_MICROS_2 = 10957 * Interval::MICROS_PER_DAY;

	enum struct BucketWidthType { LessThanDays, MoreThanMonths, Unclassified };

	static inline BucketWidthType ClassifyBucketWidth(const interval_t bucket_width) {
		if (bucket_width.months == 0 && Interval::GetMicro(bucket_width) > 0) {
			return BucketWidthType::LessThanDays;
		} else if (bucket_width.months > 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
			return BucketWidthType::MoreThanMonths;
		} else {
			return BucketWidthType::Unclassified;
		}
	}

	static inline BucketWidthType ClassifyBucketWidthWithThrowingError(const interval_t bucket_width) {
		if (bucket_width.months == 0) {
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			if (bucket_width_micros <= 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return BucketWidthType::LessThanDays;
		} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
			if (bucket_width.months < 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return BucketWidthType::MoreThanMonths;
		} else {
			throw NotImplementedException("Month intervals cannot have day or time component");
		}
	}

	static int32_t EpochMonths(timestamp_t ts, icu::Calendar *calendar) {
		SetTime(calendar, ts);
		return (ExtractField(calendar, UCAL_YEAR) - 1970) * 12 + ExtractField(calendar, UCAL_MONTH) - 1;
	}

	static timestamp_t WidthLessThanDaysCommon(int64_t bucket_width_micros, timestamp_t ts, timestamp_t origin,
	                                           icu::Calendar *calendar) {
		int64_t ts_micros = Timestamp::GetEpochMicroSeconds(ts);
		int64_t origin_micros = Timestamp::GetEpochMicroSeconds(origin);
		origin_micros %= bucket_width_micros;
		if (origin_micros > 0 && ts_micros < NumericLimits<int64_t>::Minimum() + origin_micros) {
			throw OutOfRangeException("Timestamp out of range");
		}
		if (origin_micros < 0 && ts_micros > NumericLimits<int64_t>::Maximum() + origin_micros) {
			throw OutOfRangeException("Timestamp out of range");
		}
		ts_micros -= origin_micros;

		int64_t diff_micros = (ts_micros / bucket_width_micros) * bucket_width_micros;
		if (ts_micros < 0 && ts_micros % bucket_width_micros != 0) {
			if (diff_micros < NumericLimits<int64_t>::Minimum() + bucket_width_micros) {
				throw OutOfRangeException("Timestamp out of range");
			}
			diff_micros -= bucket_width_micros;
		}

		interval_t diff = Interval::FromMicro(diff_micros);

		return ICUCalendarAdd::Operation<timestamp_t, interval_t, timestamp_t>(
		    Timestamp::FromEpochMicroSeconds(origin_micros), diff, calendar);
	}

	static timestamp_t WidthMoreThanMonthsCommon(int32_t bucket_width_months, timestamp_t ts, timestamp_t origin,
	                                             icu::Calendar *calendar) {
		int32_t ts_months = EpochMonths(ts, calendar);
		int32_t origin_months = EpochMonths(ts, calendar);
		origin_months %= bucket_width_months;
		if (origin_months > 0 && ts_months < NumericLimits<int32_t>::Minimum() + origin_months) {
			throw NotImplementedException("Timestamp out of range");
		}
		if (origin_months < 0 && ts_months > NumericLimits<int32_t>::Maximum() + origin_months) {
			throw NotImplementedException("Timestamp out of range");
		}
		ts_months -= origin_months;

		int32_t diff_months = (ts_months / bucket_width_months) * bucket_width_months;
		if (ts_months < 0 && ts_months % bucket_width_months != 0) {
			if (diff_months < NumericLimits<int32_t>::Minimum() + bucket_width_months) {
				throw OutOfRangeException("Timestamp out of range");
			}
			diff_months -= bucket_width_months;
		}

		interval_t result_months = interval_t {diff_months, 0, 0};

		return ICUCalendarAdd::Operation<timestamp_t, interval_t, timestamp_t>(timestamp_t::epoch(), result_months,
		                                                                       calendar);
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
		static inline TR Operation(TA bucket_width, TB ts, TC tz, icu::Calendar *calendar) {
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
timestamp_t ICUTimeBucket::WidthLessThanDaysBinaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                      icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
	timestamp_t origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_1);

	return WidthLessThanDaysCommon(bucket_width_micros, ts, origin, calendar);
}

template <>
timestamp_t ICUTimeBucket::WidthMoreThanMonthsBinaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                        icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	timestamp_t origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_2);

	return WidthMoreThanMonthsCommon(bucket_width.months, ts, origin, calendar);
}

template <>
timestamp_t ICUTimeBucket::BinaryOperator::Operation(interval_t bucket_width, timestamp_t ts, icu::Calendar *calendar) {
	BucketWidthType bucket_width_type = ClassifyBucketWidthWithThrowingError(bucket_width);
	switch (bucket_width_type) {
	case BucketWidthType::LessThanDays:
		return WidthLessThanDaysBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(bucket_width, ts,
		                                                                                        calendar);
	default:
		return WidthMoreThanMonthsBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(bucket_width, ts,
		                                                                                          calendar);
	}
}

template <>
timestamp_t ICUTimeBucket::OffsetWidthLessThanDaysTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                             interval_t offset,
                                                                             icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
	ts = ICUCalendarSub::Operation<timestamp_t, interval_t, timestamp_t>(ts, offset, calendar);
	timestamp_t origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_1);

	return ICUCalendarAdd::Operation<timestamp_t, interval_t, timestamp_t>(
	    WidthLessThanDaysCommon(bucket_width_micros, ts, origin, calendar), offset, calendar);
}

template <>
timestamp_t ICUTimeBucket::OffsetWidthMoreThanMonthsTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                               interval_t offset,
                                                                               icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	ts = ICUCalendarSub::Operation<timestamp_t, interval_t, timestamp_t>(ts, offset, calendar);
	timestamp_t origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_2);

	return ICUCalendarAdd::Operation<timestamp_t, interval_t, timestamp_t>(
	    WidthMoreThanMonthsCommon(bucket_width.months, ts, origin, calendar), offset, calendar);
}

template <>
timestamp_t ICUTimeBucket::OffsetTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts, interval_t offset,
                                                            icu::Calendar *calendar) {
	BucketWidthType bucket_width_type = ClassifyBucketWidthWithThrowingError(bucket_width);
	switch (bucket_width_type) {
	case BucketWidthType::LessThanDays:
		return OffsetWidthLessThanDaysTernaryOperator::Operation<interval_t, timestamp_t, interval_t, timestamp_t>(
		    bucket_width, ts, offset, calendar);
	default:
		return OffsetWidthMoreThanMonthsTernaryOperator::Operation<interval_t, timestamp_t, interval_t, timestamp_t>(
		    bucket_width, ts, offset, calendar);
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

	return WidthLessThanDaysCommon(bucket_width_micros, ts, origin, calendar);
}

template <>
timestamp_t ICUTimeBucket::OriginWidthMoreThanMonthsTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                               timestamp_t origin,
                                                                               icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}

	return WidthMoreThanMonthsCommon(bucket_width.months, ts, origin, calendar);
}

template <>
timestamp_t ICUTimeBucket::OriginTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts, timestamp_t origin,
                                                            ValidityMask &mask, idx_t idx, icu::Calendar *calendar) {
	if (!Value::IsFinite(origin)) {
		mask.SetInvalid(idx);
		return timestamp_t(0);
	}
	BucketWidthType bucket_width_type = ClassifyBucketWidthWithThrowingError(bucket_width);
	switch (bucket_width_type) {
	case BucketWidthType::LessThanDays:
		return OriginWidthLessThanDaysTernaryOperator::Operation<interval_t, timestamp_t, timestamp_t, timestamp_t>(
		    bucket_width, ts, origin, calendar);
	default:
		return OriginWidthMoreThanMonthsTernaryOperator::Operation<interval_t, timestamp_t, timestamp_t, timestamp_t>(
		    bucket_width, ts, origin, calendar);
	}
}

template <>
timestamp_t ICUTimeBucket::TimeZoneWidthLessThanDaysBinaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                              icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
	timestamp_t origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_1);

	return WidthLessThanDaysCommon(bucket_width_micros, ts, origin, calendar);
}

template <>
timestamp_t ICUTimeBucket::TimeZoneWidthMoreThanMonthsBinaryOperator::Operation(interval_t bucket_width, timestamp_t ts,
                                                                                icu::Calendar *calendar) {
	if (!Value::IsFinite(ts)) {
		return ts;
	}
	timestamp_t origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_2);

	return WidthMoreThanMonthsCommon(bucket_width.months, ts, origin, calendar);
}

template <>
timestamp_t ICUTimeBucket::TimeZoneTernaryOperator::Operation(interval_t bucket_width, timestamp_t ts, string_t tz,
                                                              icu::Calendar *calendar) {
	SetTimeZone(calendar, tz);
	BucketWidthType bucket_width_type = ClassifyBucketWidthWithThrowingError(bucket_width);
	switch (bucket_width_type) {
	case BucketWidthType::LessThanDays:
		return TimeZoneWidthLessThanDaysBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(bucket_width,
		                                                                                                ts, calendar);
	default:
		return TimeZoneWidthMoreThanMonthsBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(bucket_width,
		                                                                                                  ts, calendar);
	}
}

void ICUTimeBucket::ICUTimeBucketFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (BindData &)*func_expr.bind_info;
	CalendarPtr calendar_ptr(info.calendar->clone());
	auto calendar = calendar_ptr.get();

	auto &bucket_width_arg = args.data[0];
	auto &ts_arg = args.data[1];

	if (bucket_width_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(bucket_width_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			interval_t bucket_width = *ConstantVector::GetData<interval_t>(bucket_width_arg);
			BucketWidthType bucket_width_type = ClassifyBucketWidth(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::LessThanDays:
				BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
					    return WidthLessThanDaysBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(
					        bucket_width, ts, calendar);
				    });
				break;
			case BucketWidthType::MoreThanMonths:
				BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
					    return WidthMoreThanMonthsBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(
					        bucket_width, ts, calendar);
				    });
				break;
			default:
				BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
					    return BinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(bucket_width, ts,
					                                                                           calendar);
				    });
				break;
			}
		}
	} else {
		BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
		    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
			    return BinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(bucket_width, ts, calendar);
		    });
	}
}

void ICUTimeBucket::ICUTimeBucketOffsetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (BindData &)*func_expr.bind_info;
	CalendarPtr calendar_ptr(info.calendar->clone());
	auto calendar = calendar_ptr.get();

	auto &bucket_width_arg = args.data[0];
	auto &ts_arg = args.data[1];
	auto &offset_arg = args.data[2];

	if (bucket_width_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(bucket_width_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			interval_t bucket_width = *ConstantVector::GetData<interval_t>(bucket_width_arg);
			BucketWidthType bucket_width_type = ClassifyBucketWidth(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::LessThanDays:
				TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
					    return OffsetWidthLessThanDaysTernaryOperator::Operation<interval_t, timestamp_t, interval_t,
					                                                             timestamp_t>(bucket_width, ts, offset,
					                                                                          calendar);
				    });
				break;
			case BucketWidthType::MoreThanMonths:
				TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
					    return OffsetWidthMoreThanMonthsTernaryOperator::Operation<interval_t, timestamp_t, interval_t,
					                                                               timestamp_t>(bucket_width, ts,
					                                                                            offset, calendar);
				    });
				break;
			default:
				TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
					    return OffsetTernaryOperator::Operation<interval_t, timestamp_t, interval_t, timestamp_t>(
					        bucket_width, ts, offset, calendar);
				    });
				break;
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

void ICUTimeBucket::ICUTimeBucketOriginFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);

	auto &func_expr = (BoundFunctionExpression &)state.expr;
	auto &info = (BindData &)*func_expr.bind_info;
	CalendarPtr calendar_ptr(info.calendar->clone());
	auto calendar = calendar_ptr.get();

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
			BucketWidthType bucket_width_type = ClassifyBucketWidth(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::LessThanDays:
				TernaryExecutor::Execute<interval_t, timestamp_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin) {
					    return OriginWidthLessThanDaysTernaryOperator::Operation<interval_t, timestamp_t, timestamp_t,
					                                                             timestamp_t>(bucket_width, ts, origin,
					                                                                          calendar);
				    });
				break;
			case BucketWidthType::MoreThanMonths:
				TernaryExecutor::Execute<interval_t, timestamp_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin) {
					    return OriginWidthMoreThanMonthsTernaryOperator::Operation<interval_t, timestamp_t, timestamp_t,
					                                                               timestamp_t>(bucket_width, ts,
					                                                                            origin, calendar);
				    });
				break;
			default:
				TernaryExecutor::ExecuteWithNulls<interval_t, timestamp_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin, ValidityMask &mask, idx_t idx) {
					    return OriginTernaryOperator::Operation<interval_t, timestamp_t, timestamp_t, timestamp_t>(
					        bucket_width, ts, origin, mask, idx, calendar);
				    });
				break;
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

	if (bucket_width_arg.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	    tz_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(bucket_width_arg) || ConstantVector::IsNull(tz_arg)) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			interval_t bucket_width = *ConstantVector::GetData<interval_t>(bucket_width_arg);
			SetTimeZone(calendar, *ConstantVector::GetData<string_t>(tz_arg));
			BucketWidthType bucket_width_type = ClassifyBucketWidth(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::LessThanDays:
				BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
					    return TimeZoneWidthLessThanDaysBinaryOperator::Operation<interval_t, timestamp_t, timestamp_t>(
					        bucket_width, ts, calendar);
				    });
				break;
			case BucketWidthType::MoreThanMonths:
				BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
				    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
					    return TimeZoneWidthMoreThanMonthsBinaryOperator::Operation<interval_t, timestamp_t,
					                                                                timestamp_t>(bucket_width, ts,
					                                                                             calendar);
				    });
				break;
			default:
				TernaryExecutor::Execute<interval_t, timestamp_t, string_t, timestamp_t>(
				    bucket_width_arg, ts_arg, tz_arg, result, args.size(),
				    [&](interval_t bucket_width, timestamp_t ts, string_t tz) {
					    return TimeZoneTernaryOperator::Operation<interval_t, timestamp_t, string_t, timestamp_t>(
					        bucket_width, ts, tz, calendar);
				    });
				break;
			}
		}
	} else {
		TernaryExecutor::Execute<interval_t, timestamp_t, string_t, timestamp_t>(
		    bucket_width_arg, ts_arg, tz_arg, result, args.size(),
		    [&](interval_t bucket_width, timestamp_t ts, string_t tz) {
			    return TimeZoneTernaryOperator::Operation<interval_t, timestamp_t, string_t, timestamp_t>(
			        bucket_width, ts, tz, calendar);
		    });
	}
}

void ICUTimeBucket::AddTimeBucketFunction(const string &name, ClientContext &context) {
	ScalarFunctionSet set("time_bucket");
	set.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP_TZ}, LogicalType::TIMESTAMP_TZ,
	                               ICUTimeBucketFunction, Bind));
	set.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP_TZ, LogicalType::INTERVAL},
	                               LogicalType::TIMESTAMP_TZ, ICUTimeBucketOffsetFunction, Bind));
	set.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ},
	                               LogicalType::TIMESTAMP_TZ, ICUTimeBucketOriginFunction, Bind));
	set.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP_TZ, LogicalType::VARCHAR},
	                               LogicalType::TIMESTAMP_TZ, ICUTimeBucketTimeZoneFunction, Bind));

	CreateScalarFunctionInfo func_info(set);
	auto &catalog = Catalog::GetCatalog(context);
	catalog.AddFunction(context, &func_info);
}

void RegisterICUTimeBucketFunctions(ClientContext &context) {
	ICUTimeBucket::AddTimeBucketFunction("time_bucket", context);
}

} // namespace duckdb
