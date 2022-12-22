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

struct TimeBucket {

	// Use 2000-01-03 00:00:00 (Monday) as origin when bucket_width is days, hours, ... for TimescaleDB compatibility
	// There are 10959 days between 1970-01-01 and 2000-01-03
	constexpr static const int64_t DEFAULT_ORIGIN_MICROS = 10959 * Interval::MICROS_PER_DAY;
	// Use 2000-01-01 as origin when bucket_width is months, years, ... for TimescaleDB compatibility
	// There are 360 months between 1970-01-01 and 2000-01-01
	constexpr static const int32_t DEFAULT_ORIGIN_MONTHS = 360;

	template <typename T>
	static int32_t EpochMonths(T ts) {
		date_t ts_date = Cast::template Operation<T, date_t>(ts);
		return (Date::ExtractYear(ts_date) - 1970) * 12 + Date::ExtractMonth(ts_date) - 1;
	}

	static timestamp_t WidthLessThanDaysCommon(int64_t bucket_width_micros, int64_t ts_micros, int64_t origin_micros) {
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

		return Timestamp::FromEpochMicroSeconds(result_micros);
	}

	static date_t WidthMoreThanMonthsCommon(int32_t bucket_width_months, int32_t ts_months, int32_t origin_months) {
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

		int32_t year =
		    (result_months < 0 && result_months % 12 != 0) ? 1970 + result_months / 12 - 1 : 1970 + result_months / 12;
		int32_t month =
		    (result_months < 0 && result_months % 12 != 0) ? result_months % 12 + 13 : result_months % 12 + 1;

		return Date::FromDate(year, month, 1);
	}

	struct WidthLessThanDaysBinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(ts));
			return Cast::template Operation<timestamp_t, TR>(
			    WidthLessThanDaysCommon(bucket_width_micros, ts_micros, DEFAULT_ORIGIN_MICROS));
		}
	};

	struct WidthMoreThanMonthsBinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int32_t ts_months = EpochMonths(ts);
			return Cast::template Operation<date_t, TR>(
			    WidthMoreThanMonthsCommon(bucket_width.months, ts_months, DEFAULT_ORIGIN_MONTHS));
		}
	};

	struct BinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts) {
			if (bucket_width.months == 0) {
				int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
				if (bucket_width_micros <= 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				return WidthLessThanDaysBinaryOperator::Operation<TA, TB, TR>(bucket_width, ts);
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				return WidthMoreThanMonthsBinaryOperator::Operation<TA, TB, TR>(bucket_width, ts);
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
			}
		}
	};

	struct OffsetWidthLessThanDaysTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(
			    Interval::Add(Cast::template Operation<TB, timestamp_t>(ts), Interval::Invert(offset)));
			return Cast::template Operation<timestamp_t, TR>(
			    Interval::Add(WidthLessThanDaysCommon(bucket_width_micros, ts_micros, DEFAULT_ORIGIN_MICROS), offset));
		}
	};

	struct OffsetWidthMoreThanMonthsTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int32_t ts_months = EpochMonths(Interval::Add(ts, Interval::Invert(offset)));
			return Interval::Add(Cast::template Operation<date_t, TR>(
			                         WidthMoreThanMonthsCommon(bucket_width.months, ts_months, DEFAULT_ORIGIN_MONTHS)),
			                     offset);
		}
	};

	struct OffsetTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset) {
			if (bucket_width.months == 0) {
				int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
				if (bucket_width_micros <= 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				return OffsetWidthLessThanDaysTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts, offset);
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				return OffsetWidthMoreThanMonthsTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts, offset);
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
			}
		}
	};

	struct OriginWidthLessThanDaysTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC origin) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(ts));
			int64_t origin_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(origin));

			return Cast::template Operation<timestamp_t, TR>(
			    WidthLessThanDaysCommon(bucket_width_micros, ts_micros, origin_micros));
		}
	};

	struct OriginWidthMoreThanMonthsTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC origin) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int32_t ts_months = EpochMonths(ts);
			int32_t origin_months = EpochMonths(origin);

			return Cast::template Operation<date_t, TR>(
			    WidthMoreThanMonthsCommon(bucket_width.months, ts_months, origin_months));
		}
	};

	struct OriginTernaryOperator {
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
				return OriginWidthLessThanDaysTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts, origin);
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				return OriginWidthMoreThanMonthsTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts, origin);
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
			}
		}
	};
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
			if (bucket_width.months == 0 && Interval::GetMicro(bucket_width) > 0) {
				BinaryExecutor::Execute<interval_t, T, T>(
				    bucket_width_arg, ts_arg, result, args.size(),
				    TimeBucket::WidthLessThanDaysBinaryOperator::Operation<interval_t, T, T>);
			} else if (bucket_width.months > 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				BinaryExecutor::Execute<interval_t, T, T>(
				    bucket_width_arg, ts_arg, result, args.size(),
				    TimeBucket::WidthMoreThanMonthsBinaryOperator::Operation<interval_t, T, T>);
			} else {
				BinaryExecutor::Execute<interval_t, T, T>(bucket_width_arg, ts_arg, result, args.size(),
				                                          TimeBucket::BinaryOperator::Operation<interval_t, T, T>);
			}
		}
	} else {
		BinaryExecutor::Execute<interval_t, T, T>(bucket_width_arg, ts_arg, result, args.size(),
		                                          TimeBucket::BinaryOperator::Operation<interval_t, T, T>);
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
			if (bucket_width.months == 0 && Interval::GetMicro(bucket_width) > 0) {
				TernaryExecutor::Execute<interval_t, T, interval_t, T>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    TimeBucket::OffsetWidthLessThanDaysTernaryOperator::Operation<interval_t, T, interval_t, T>);
			} else if (bucket_width.months > 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				TernaryExecutor::Execute<interval_t, T, interval_t, T>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    TimeBucket::OffsetWidthMoreThanMonthsTernaryOperator::Operation<interval_t, T, interval_t, T>);
			} else {
				TernaryExecutor::Execute<interval_t, T, interval_t, T>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    TimeBucket::OffsetTernaryOperator::Operation<interval_t, T, interval_t, T>);
			}
		}
	} else {
		TernaryExecutor::Execute<interval_t, T, interval_t, T>(
		    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
		    TimeBucket::OffsetTernaryOperator::Operation<interval_t, T, interval_t, T>);
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
			if (bucket_width.months == 0 && Interval::GetMicro(bucket_width) > 0) {
				TernaryExecutor::Execute<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucket::OriginWidthLessThanDaysTernaryOperator::Operation<interval_t, T, T, T>);
			} else if (bucket_width.months > 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				TernaryExecutor::Execute<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucket::OriginWidthMoreThanMonthsTernaryOperator::Operation<interval_t, T, T, T>);
			} else {
				TernaryExecutor::ExecuteWithNulls<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucket::OriginTernaryOperator::Operation<interval_t, T, T, T>);
			}
		}
	} else {
		TernaryExecutor::ExecuteWithNulls<interval_t, T, T, T>(
		    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
		    TimeBucket::OriginTernaryOperator::Operation<interval_t, T, T, T>);
	}
}

void TimeBucketFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet time_bucket("time_bucket");
	time_bucket.AddFunction(
	    ScalarFunction({LogicalType::INTERVAL, LogicalType::DATE}, LogicalType::DATE, TimeBucketFunction<date_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP}, LogicalType::TIMESTAMP,
	                                       TimeBucketFunction<timestamp_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::DATE, LogicalType::INTERVAL},
	                                       LogicalType::DATE, TimeBucketOffsetFunction<date_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP, LogicalType::INTERVAL},
	                                       LogicalType::TIMESTAMP, TimeBucketOffsetFunction<timestamp_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::DATE, LogicalType::DATE},
	                                       LogicalType::DATE, TimeBucketOriginFunction<date_t>));
	time_bucket.AddFunction(ScalarFunction({LogicalType::INTERVAL, LogicalType::TIMESTAMP, LogicalType::TIMESTAMP},
	                                       LogicalType::TIMESTAMP, TimeBucketOriginFunction<timestamp_t>));

	set.AddFunction(time_bucket);
}

} // namespace duckdb
