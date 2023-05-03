#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/core_functions/scalar/date_functions.hpp"

namespace duckdb {

struct TimeBucket {

	// Use 2000-01-03 00:00:00 (Monday) as origin when bucket_width is days, hours, ... for TimescaleDB compatibility
	// There are 10959 days between 1970-01-01 and 2000-01-03
	constexpr static const int64_t DEFAULT_ORIGIN_MICROS = 10959 * Interval::MICROS_PER_DAY;
	// Use 2000-01-01 as origin when bucket_width is months, years, ... for TimescaleDB compatibility
	// There are 360 months between 1970-01-01 and 2000-01-01
	constexpr static const int32_t DEFAULT_ORIGIN_MONTHS = 360;

	enum struct BucketWidthType { CONVERTIBLE_TO_MICROS, CONVERTIBLE_TO_MONTHS, UNCLASSIFIED };

	static inline BucketWidthType ClassifyBucketWidth(const interval_t bucket_width) {
		if (bucket_width.months == 0 && Interval::GetMicro(bucket_width) > 0) {
			return BucketWidthType::CONVERTIBLE_TO_MICROS;
		} else if (bucket_width.months > 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
			return BucketWidthType::CONVERTIBLE_TO_MONTHS;
		} else {
			return BucketWidthType::UNCLASSIFIED;
		}
	}

	static inline BucketWidthType ClassifyBucketWidthErrorThrow(const interval_t bucket_width) {
		if (bucket_width.months == 0) {
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			if (bucket_width_micros <= 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return BucketWidthType::CONVERTIBLE_TO_MICROS;
		} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
			if (bucket_width.months < 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return BucketWidthType::CONVERTIBLE_TO_MONTHS;
		} else {
			throw NotImplementedException("Month intervals cannot have day or time component");
		}
	}

	template <typename T>
	static inline int32_t EpochMonths(T ts) {
		date_t ts_date = Cast::template Operation<T, date_t>(ts);
		return (Date::ExtractYear(ts_date) - 1970) * 12 + Date::ExtractMonth(ts_date) - 1;
	}

	static inline timestamp_t WidthConvertibleToMicrosCommon(int64_t bucket_width_micros, int64_t ts_micros,
	                                                         int64_t origin_micros) {
		origin_micros %= bucket_width_micros;
		ts_micros = SubtractOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(ts_micros, origin_micros);

		int64_t result_micros = (ts_micros / bucket_width_micros) * bucket_width_micros;
		if (ts_micros < 0 && ts_micros % bucket_width_micros != 0) {
			result_micros =
			    SubtractOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(result_micros, bucket_width_micros);
		}
		result_micros += origin_micros;

		return Timestamp::FromEpochMicroSeconds(result_micros);
	}

	static inline date_t WidthConvertibleToMonthsCommon(int32_t bucket_width_months, int32_t ts_months,
	                                                    int32_t origin_months) {
		origin_months %= bucket_width_months;
		ts_months = SubtractOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(ts_months, origin_months);

		int32_t result_months = (ts_months / bucket_width_months) * bucket_width_months;
		if (ts_months < 0 && ts_months % bucket_width_months != 0) {
			result_months =
			    SubtractOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(result_months, bucket_width_months);
		}
		result_months += origin_months;

		int32_t year =
		    (result_months < 0 && result_months % 12 != 0) ? 1970 + result_months / 12 - 1 : 1970 + result_months / 12;
		int32_t month =
		    (result_months < 0 && result_months % 12 != 0) ? result_months % 12 + 13 : result_months % 12 + 1;

		return Date::FromDate(year, month, 1);
	}

	struct WidthConvertibleToMicrosBinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(ts));
			return Cast::template Operation<timestamp_t, TR>(
			    WidthConvertibleToMicrosCommon(bucket_width_micros, ts_micros, DEFAULT_ORIGIN_MICROS));
		}
	};

	struct WidthConvertibleToMonthsBinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int32_t ts_months = EpochMonths(ts);
			return Cast::template Operation<date_t, TR>(
			    WidthConvertibleToMonthsCommon(bucket_width.months, ts_months, DEFAULT_ORIGIN_MONTHS));
		}
	};

	struct BinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts) {
			BucketWidthType bucket_width_type = ClassifyBucketWidthErrorThrow(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::CONVERTIBLE_TO_MICROS:
				return WidthConvertibleToMicrosBinaryOperator::Operation<TA, TB, TR>(bucket_width, ts);
			case BucketWidthType::CONVERTIBLE_TO_MONTHS:
				return WidthConvertibleToMonthsBinaryOperator::Operation<TA, TB, TR>(bucket_width, ts);
			default:
				throw NotImplementedException("Bucket type not implemented for TIME_BUCKET");
			}
		}
	};

	struct OffsetWidthConvertibleToMicrosTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(
			    Interval::Add(Cast::template Operation<TB, timestamp_t>(ts), Interval::Invert(offset)));
			return Cast::template Operation<timestamp_t, TR>(Interval::Add(
			    WidthConvertibleToMicrosCommon(bucket_width_micros, ts_micros, DEFAULT_ORIGIN_MICROS), offset));
		}
	};

	struct OffsetWidthConvertibleToMonthsTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int32_t ts_months = EpochMonths(Interval::Add(ts, Interval::Invert(offset)));
			return Interval::Add(Cast::template Operation<date_t, TR>(WidthConvertibleToMonthsCommon(
			                         bucket_width.months, ts_months, DEFAULT_ORIGIN_MONTHS)),
			                     offset);
		}
	};

	struct OffsetTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset) {
			BucketWidthType bucket_width_type = ClassifyBucketWidthErrorThrow(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::CONVERTIBLE_TO_MICROS:
				return OffsetWidthConvertibleToMicrosTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts,
				                                                                                offset);
			case BucketWidthType::CONVERTIBLE_TO_MONTHS:
				return OffsetWidthConvertibleToMonthsTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts,
				                                                                                offset);
			default:
				throw NotImplementedException("Bucket type not implemented for TIME_BUCKET");
			}
		}
	};

	struct OriginWidthConvertibleToMicrosTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC origin) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(ts));
			int64_t origin_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(origin));
			return Cast::template Operation<timestamp_t, TR>(
			    WidthConvertibleToMicrosCommon(bucket_width_micros, ts_micros, origin_micros));
		}
	};

	struct OriginWidthConvertibleToMonthsTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC origin) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int32_t ts_months = EpochMonths(ts);
			int32_t origin_months = EpochMonths(origin);
			return Cast::template Operation<date_t, TR>(
			    WidthConvertibleToMonthsCommon(bucket_width.months, ts_months, origin_months));
		}
	};

	struct OriginTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC origin, ValidityMask &mask, idx_t idx) {
			if (!Value::IsFinite(origin)) {
				mask.SetInvalid(idx);
				return TR();
			}
			BucketWidthType bucket_width_type = ClassifyBucketWidthErrorThrow(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::CONVERTIBLE_TO_MICROS:
				return OriginWidthConvertibleToMicrosTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts,
				                                                                                origin);
			case BucketWidthType::CONVERTIBLE_TO_MONTHS:
				return OriginWidthConvertibleToMonthsTernaryOperator::Operation<TA, TB, TC, TR>(bucket_width, ts,
				                                                                                origin);
			default:
				throw NotImplementedException("Bucket type not implemented for TIME_BUCKET");
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
			TimeBucket::BucketWidthType bucket_width_type = TimeBucket::ClassifyBucketWidth(bucket_width);
			switch (bucket_width_type) {
			case TimeBucket::BucketWidthType::CONVERTIBLE_TO_MICROS:
				BinaryExecutor::Execute<interval_t, T, T>(
				    bucket_width_arg, ts_arg, result, args.size(),
				    TimeBucket::WidthConvertibleToMicrosBinaryOperator::Operation<interval_t, T, T>);
				break;
			case TimeBucket::BucketWidthType::CONVERTIBLE_TO_MONTHS:
				BinaryExecutor::Execute<interval_t, T, T>(
				    bucket_width_arg, ts_arg, result, args.size(),
				    TimeBucket::WidthConvertibleToMonthsBinaryOperator::Operation<interval_t, T, T>);
				break;
			case TimeBucket::BucketWidthType::UNCLASSIFIED:
				BinaryExecutor::Execute<interval_t, T, T>(bucket_width_arg, ts_arg, result, args.size(),
				                                          TimeBucket::BinaryOperator::Operation<interval_t, T, T>);
				break;
			default:
				throw NotImplementedException("Bucket type not implemented for TIME_BUCKET");
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
			TimeBucket::BucketWidthType bucket_width_type = TimeBucket::ClassifyBucketWidth(bucket_width);
			switch (bucket_width_type) {
			case TimeBucket::BucketWidthType::CONVERTIBLE_TO_MICROS:
				TernaryExecutor::Execute<interval_t, T, interval_t, T>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    TimeBucket::OffsetWidthConvertibleToMicrosTernaryOperator::Operation<interval_t, T, interval_t, T>);
				break;
			case TimeBucket::BucketWidthType::CONVERTIBLE_TO_MONTHS:
				TernaryExecutor::Execute<interval_t, T, interval_t, T>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    TimeBucket::OffsetWidthConvertibleToMonthsTernaryOperator::Operation<interval_t, T, interval_t, T>);
				break;
			case TimeBucket::BucketWidthType::UNCLASSIFIED:
				TernaryExecutor::Execute<interval_t, T, interval_t, T>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    TimeBucket::OffsetTernaryOperator::Operation<interval_t, T, interval_t, T>);
				break;
			default:
				throw NotImplementedException("Bucket type not implemented for TIME_BUCKET");
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
			TimeBucket::BucketWidthType bucket_width_type = TimeBucket::ClassifyBucketWidth(bucket_width);
			switch (bucket_width_type) {
			case TimeBucket::BucketWidthType::CONVERTIBLE_TO_MICROS:
				TernaryExecutor::Execute<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucket::OriginWidthConvertibleToMicrosTernaryOperator::Operation<interval_t, T, T, T>);
				break;
			case TimeBucket::BucketWidthType::CONVERTIBLE_TO_MONTHS:
				TernaryExecutor::Execute<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucket::OriginWidthConvertibleToMonthsTernaryOperator::Operation<interval_t, T, T, T>);
				break;
			case TimeBucket::BucketWidthType::UNCLASSIFIED:
				TernaryExecutor::ExecuteWithNulls<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucket::OriginTernaryOperator::Operation<interval_t, T, T, T>);
				break;
			default:
				throw NotImplementedException("Bucket type not implemented for TIME_BUCKET");
			}
		}
	} else {
		TernaryExecutor::ExecuteWithNulls<interval_t, T, T, T>(
		    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
		    TimeBucket::OriginTernaryOperator::Operation<interval_t, T, T, T>);
	}
}

ScalarFunctionSet TimeBucketFun::GetFunctions() {
	ScalarFunctionSet time_bucket;
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
	return time_bucket;
}

} // namespace duckdb
