#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/function/scalar/date_functions.hpp"

namespace duckdb {

static int64_t TimeBucketCommonWidthLessThanDays(int64_t bucket_width_micros, int64_t ts_micros,
                                                 int64_t origin_micros) {
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

static int32_t TimeBucketCommonWidthMoreThanMonths(int32_t bucket_width_months, int32_t ts_months,
                                                   int32_t origin_months) {
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

struct TimeBucketWidthLessThanDaysBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA bucket_width, TB ts) {
		if (!Value::IsFinite(ts)) {
			return Cast::template Operation<TB, TR>(ts);
		}
		int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
		int64_t ts_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(ts));
		// set origin 2000-01-03 00:00:00 (Monday) for TimescaleDB compatibility
		// there are 10959 days between 1970-01-01 00:00:00 and 2000-01-03 00:00:00
		int64_t origin_micros = 10959 * Interval::MICROS_PER_DAY;

		return Cast::template Operation<timestamp_t, TR>(Timestamp::FromEpochMicroSeconds(
		    TimeBucketCommonWidthLessThanDays(bucket_width_micros, ts_micros, origin_micros)));
	}
};

struct TimeBucketWidthMoreThanMonthsBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA bucket_width, TB ts) {
		if (!Value::IsFinite(ts)) {
			return Cast::template Operation<TB, TR>(ts);
		}
		int32_t ts_months = (Date::ExtractYear(ts) - 1970) * 12 + Date::ExtractMonth(ts) - 1;
		// set origin 2000-01-01 00:00:00 for TimescaleDB compatibility
		// there are 360 months between 1970-01-01 00:00:00 and 2000-01-01 00:00:00
		int32_t origin_months = 360;

		int32_t result_months = TimeBucketCommonWidthMoreThanMonths(bucket_width.months, ts_months, origin_months);
		int32_t year =
		    (result_months < 0 && result_months % 12 != 0) ? 1970 + result_months / 12 - 1 : 1970 + result_months / 12;
		int32_t month =
		    (result_months < 0 && result_months % 12 != 0) ? result_months % 12 + 13 : result_months % 12 + 1;
		return Cast::template Operation<date_t, TR>(Date::FromDate(year, month, 1));
	}
};

struct TimeBucketBinaryOperator {
	template <class TA, class TB, class TR>
	static inline TR Operation(TA bucket_width, TB ts) {
		if (bucket_width.months == 0) {
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			if (bucket_width_micros <= 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return TimeBucketWidthLessThanDaysBinaryOperator::Operation<TA, TB, TR>(bucket_width, ts);
		} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
			if (bucket_width.months < 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return TimeBucketWidthMoreThanMonthsBinaryOperator::Operation<TA, TB, TR>(bucket_width, ts);
		} else {
			throw NotImplementedException("Month intervals cannot have day or time component");
		}
	}
};

struct TimeBucketOffsetWidthLessThanDaysTernaryOperator {
	template <class TA, class TB, class TC, class TR>
	static inline TR Operation(TA bucket_width, TB ts, TC offset) {
		if (!Value::IsFinite(ts)) {
			return Cast::template Operation<TB, TR>(ts);
		}
		int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
		timestamp_t ts_offsetted =
		    Interval::Add(Cast::template Operation<TB, timestamp_t>(ts), Interval::Invert(offset));
		int64_t ts_micros = Timestamp::GetEpochMicroSeconds(ts_offsetted);
		// set origin 2000-01-03 00:00:00 (Monday) for TimescaleDB compatibility
		// there are 10959 days between 1970-01-01 00:00:00 and 2000-01-03 00:00:00
		int64_t origin_micros = 10959 * Interval::MICROS_PER_DAY;

		return Cast::template Operation<timestamp_t, TR>(
		    Interval::Add(Timestamp::FromEpochMicroSeconds(
		                      TimeBucketCommonWidthLessThanDays(bucket_width_micros, ts_micros, origin_micros)),
		                  offset));
	}
};

struct TimeBucketOffsetWidthMoreThanMonthsTernaryOperator {
	template <class TA, class TB, class TC, class TR>
	static inline TR Operation(TA bucket_width, TB ts, TC offset) {
		if (!Value::IsFinite(ts)) {
			return Cast::template Operation<TB, TR>(ts);
		}
		ts = Interval::Add(ts, Interval::Invert(offset));
		int32_t ts_months = (Date::ExtractYear(ts) - 1970) * 12 + Date::ExtractMonth(ts) - 1;
		// set origin 2000-01-01 00:00:00 for TimescaleDB compatibility
		// there are 360 months between 1970-01-01 00:00:00 and 2000-01-01 00:00:00
		int32_t origin_months = 360;

		int32_t result_months = TimeBucketCommonWidthMoreThanMonths(bucket_width.months, ts_months, origin_months);
		int32_t year =
		    (result_months < 0 && result_months % 12 != 0) ? 1970 + result_months / 12 - 1 : 1970 + result_months / 12;
		int32_t month =
		    (result_months < 0 && result_months % 12 != 0) ? result_months % 12 + 13 : result_months % 12 + 1;
		return Interval::Add(Cast::template Operation<date_t, TR>(Date::FromDate(year, month, 1)), offset);
	}
};

struct TimeBucketOffsetTernaryOperator {
	template <class TA, class TB, class TC, class TR>
	static inline TR Operation(TA bucket_width, TB ts, TC offset) {
		if (bucket_width.months == 0) {
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			if (bucket_width_micros <= 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return TimeBucketOffsetWidthLessThanDaysTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts,
			                                                                                   offset);
		} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
			if (bucket_width.months < 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return TimeBucketOffsetWidthMoreThanMonthsTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts,
			                                                                                     offset);
		} else {
			throw NotImplementedException("Month intervals cannot have day or time component");
		}
	}
};

struct TimeBucketOriginWidthLessThanDaysTernaryOperator {
	template <class TA, class TB, class TC, class TR>
	static inline TR Operation(TA bucket_width, TB ts, TC origin) {
		if (!Value::IsFinite(ts)) {
			return Cast::template Operation<TB, TR>(ts);
		}
		int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
		int64_t ts_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(ts));
		int64_t origin_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(origin));

		return Cast::template Operation<timestamp_t, TR>(Timestamp::FromEpochMicroSeconds(
		    TimeBucketCommonWidthLessThanDays(bucket_width_micros, ts_micros, origin_micros)));
	}
};

struct TimeBucketOriginWidthMoreThanMonthsTernaryOperator {
	template <class TA, class TB, class TC, class TR>
	static inline TR Operation(TA bucket_width, TB ts, TC origin) {
		if (!Value::IsFinite(ts)) {
			return Cast::template Operation<TB, TR>(ts);
		}
		int32_t ts_months = (Date::ExtractYear(ts) - 1970) * 12 + Date::ExtractMonth(ts) - 1;
		int32_t origin_months = (Date::ExtractYear(origin) - 1970) * 12 + Date::ExtractMonth(origin) - 1;

		int32_t result_months = TimeBucketCommonWidthMoreThanMonths(bucket_width.months, ts_months, origin_months);
		int32_t year =
		    (result_months < 0 && result_months % 12 != 0) ? 1970 + result_months / 12 - 1 : 1970 + result_months / 12;
		int32_t month =
		    (result_months < 0 && result_months % 12 != 0) ? result_months % 12 + 13 : result_months % 12 + 1;
		return Cast::template Operation<date_t, TR>(Date::FromDate(year, month, 1));
	}
};

struct TimeBucketOriginTernaryOperator {
	template <class TA, class TB, class TC, class TR>
	static inline TR Operation(TA bucket_width, TB ts, TC origin, ValidityMask &mask, idx_t idx) {
		if (!Value::IsFinite(origin)) {
			mask.SetInvalid(idx);
			return TR();
		}
		if (bucket_width.months == 0) {
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			if (bucket_width_micros <= 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return TimeBucketOriginWidthLessThanDaysTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts,
			                                                                                   origin);
		} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
			if (bucket_width.months < 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return TimeBucketOriginWidthMoreThanMonthsTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts,
			                                                                                     origin);
		} else {
			throw NotImplementedException("Month intervals cannot have day or time component");
		}
	}
};

template <typename T>
static void TimeBucketFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 2);

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
				BinaryExecutor::Execute<interval_t, T, T>(
				    bucket_width_arg, ts_arg, result, args.size(),
				    TimeBucketWidthLessThanDaysBinaryOperator::Operation<interval_t, T, T>);
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				BinaryExecutor::Execute<interval_t, T, T>(
				    bucket_width_arg, ts_arg, result, args.size(),
				    TimeBucketWidthMoreThanMonthsBinaryOperator::Operation<interval_t, T, T>);
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
			}
		}
	} else {
		BinaryExecutor::ExecuteStandard<interval_t, T, T, TimeBucketBinaryOperator>(bucket_width_arg, ts_arg, result,
		                                                                            args.size());
	}
}

template <typename T>
static void TimeBucketOffsetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);

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
				TernaryExecutor::Execute<interval_t, T, interval_t, T>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    TimeBucketOffsetWidthLessThanDaysTernaryOperator::Operation<interval_t, T, interval_t, T>);
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				TernaryExecutor::Execute<interval_t, T, interval_t, T>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    TimeBucketOffsetWidthMoreThanMonthsTernaryOperator::Operation<interval_t, T, interval_t, T>);
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
			}
		}
	} else {
		TernaryExecutor::Execute<interval_t, T, interval_t, T>(
		    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
		    TimeBucketOffsetTernaryOperator::Operation<interval_t, T, interval_t, T>);
	}
}

template <typename T>
static void TimeBucketOriginFunction(DataChunk &args, ExpressionState &state, Vector &result) {
	D_ASSERT(args.ColumnCount() == 3);

	auto &bucket_width_arg = args.data[0];
	auto &ts_arg = args.data[1];
	auto &origin_arg = args.data[2];

	if (bucket_width_arg.GetVectorType() == VectorType::CONSTANT_VECTOR &&
	    origin_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
		if (ConstantVector::IsNull(bucket_width_arg) || ConstantVector::IsNull(origin_arg) ||
		    !Value::IsFinite(*ConstantVector::GetData<T>(origin_arg))) {
			result.SetVectorType(VectorType::CONSTANT_VECTOR);
			ConstantVector::SetNull(result, true);
		} else {
			interval_t bucket_width = *ConstantVector::GetData<interval_t>(bucket_width_arg);
			if (bucket_width.months == 0) {
				int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
				if (bucket_width_micros <= 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				TernaryExecutor::Execute<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucketOriginWidthLessThanDaysTernaryOperator::Operation<interval_t, T, T, T>);
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				TernaryExecutor::Execute<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucketOriginWidthMoreThanMonthsTernaryOperator::Operation<interval_t, T, T, T>);
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
			}
		}
	} else {
		TernaryExecutor::ExecuteWithNulls<interval_t, T, T, T>(
		    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
		    TimeBucketOriginTernaryOperator::Operation<interval_t, T, T, T>);
	}
}

void TimeBucketFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet time_bucket("time_bucket");
	time_bucket.AddFunction(
	    ScalarFunction({LogicalType::INTERVAL, LogicalType::DATE}, LogicalType::DATE, TimeBucketFunction<date_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP}, LogicalType::TIMESTAMP,
	                                       TimeBucketFunction<timestamp_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP_TZ},
	                                       LogicalType::TIMESTAMP_TZ, TimeBucketFunction<timestamp_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::DATE, LogicalType::INTERVAL},
	                                       LogicalType::DATE, TimeBucketOffsetFunction<date_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP, LogicalType::INTERVAL},
	                                       LogicalType::TIMESTAMP, TimeBucketOffsetFunction<timestamp_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP_TZ, LogicalType::INTERVAL},
	                                       LogicalType::TIMESTAMP_TZ, TimeBucketOffsetFunction<timestamp_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::DATE, LogicalType::DATE},
	                                       LogicalType::DATE, TimeBucketOriginFunction<date_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP},
	                                       LogicalType::TIMESTAMP, TimeBucketOriginFunction<timestamp_t>));
	time_bucket.AddFunction(
	    ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ},
	                   LogicalType::TIMESTAMP_TZ, TimeBucketOriginFunction<timestamp_t>));

	set.AddFunction(time_bucket);
}

} // namespace duckdb
