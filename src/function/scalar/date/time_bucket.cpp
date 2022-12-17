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

	constexpr static const int64_t DEFAULT_ORIGIN_DAYS = 10959;
	constexpr static const int32_t DEFAULT_ORIGIN_MONTHS = 360;

	template <typename T>
	static int32_t EpochMonths(T ts) {
		date_t ts_date = Cast::template Operation<T, date_t>(ts);
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

	struct WidthLessThanDaysBinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(Cast::template Operation<TB, timestamp_t>(ts));
			// set origin 2000-01-03 00:00:00 (Monday) for TimescaleDB compatibility
			// there are 10959 days between 1970-01-01 00:00:00 and 2000-01-03 00:00:00
			int64_t origin_micros = DEFAULT_ORIGIN_DAYS * Interval::MICROS_PER_DAY;

			return Cast::template Operation<timestamp_t, TR>(Timestamp::FromEpochMicroSeconds(
			    WidthLessThanDaysCommon(bucket_width_micros, ts_micros, origin_micros)));
		}
	};

	struct WidthMoreThanMonthsBinaryOperator {
		template <class TA, class TB, class TR>
		static inline TR Operation(TA bucket_width, TB ts) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int32_t ts_months = EpochMonths(ts);
			// set origin 2000-01-01 00:00:00 for TimescaleDB compatibility
			// there are 360 months between 1970-01-01 00:00:00 and 2000-01-01 00:00:00
			int32_t origin_months = DEFAULT_ORIGIN_MONTHS;

			int32_t result_months = WidthMoreThanMonthsCommon(bucket_width.months, ts_months, origin_months);
			int32_t year = ExtractYearFromEpochMonths(result_months);
			int32_t month = ExtractMonthFromEpochMonths(result_months);
			return Cast::template Operation<date_t, TR>(Date::FromDate(year, month, 1));
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
			// set origin 2000-01-03 00:00:00 (Monday) for TimescaleDB compatibility
			// there are 10959 days between 1970-01-01 00:00:00 and 2000-01-03 00:00:00
			int64_t origin_micros = DEFAULT_ORIGIN_DAYS * Interval::MICROS_PER_DAY;

			return Cast::template Operation<timestamp_t, TR>(
			    Interval::Add(Timestamp::FromEpochMicroSeconds(
			                      WidthLessThanDaysCommon(bucket_width_micros, ts_micros, origin_micros)),
			                  offset));
		}
	};

	struct OffsetWidthMoreThanMonthsTernaryOperator {
		template <class TA, class TB, class TC, class TR>
		static inline TR Operation(TA bucket_width, TB ts, TC offset) {
			if (!Value::IsFinite(ts)) {
				return Cast::template Operation<TB, TR>(ts);
			}
			int32_t ts_months = EpochMonths(Interval::Add(ts, Interval::Invert(offset)));
			// set origin 2000-01-01 00:00:00 for TimescaleDB compatibility
			// there are 360 months between 1970-01-01 00:00:00 and 2000-01-01 00:00:00
			int32_t origin_months = DEFAULT_ORIGIN_MONTHS;

			int32_t result_months = WidthMoreThanMonthsCommon(bucket_width.months, ts_months, origin_months);
			int32_t year = ExtractYearFromEpochMonths(result_months);
			int32_t month = ExtractMonthFromEpochMonths(result_months);
			return Interval::Add(Cast::template Operation<date_t, TR>(Date::FromDate(year, month, 1)), offset);
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

			return Cast::template Operation<timestamp_t, TR>(Timestamp::FromEpochMicroSeconds(
			    WidthLessThanDaysCommon(bucket_width_micros, ts_micros, origin_micros)));
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

			int32_t result_months = WidthMoreThanMonthsCommon(bucket_width.months, ts_months, origin_months);
			int32_t year = ExtractYearFromEpochMonths(result_months);
			int32_t month = ExtractMonthFromEpochMonths(result_months);
			return Cast::template Operation<date_t, TR>(Date::FromDate(year, month, 1));
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
			if (bucket_width.months == 0) {
				int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
				if (bucket_width_micros <= 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				BinaryExecutor::Execute<interval_t, T, T>(
				    bucket_width_arg, ts_arg, result, args.size(),
				    TimeBucket::WidthLessThanDaysBinaryOperator::Operation<interval_t, T, T>);
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				BinaryExecutor::Execute<interval_t, T, T>(
				    bucket_width_arg, ts_arg, result, args.size(),
				    TimeBucket::WidthMoreThanMonthsBinaryOperator::Operation<interval_t, T, T>);
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
			}
		}
	} else {
		BinaryExecutor::ExecuteStandard<interval_t, T, T, TimeBucket::BinaryOperator>(bucket_width_arg, ts_arg, result,
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
				    TimeBucket::OffsetWidthLessThanDaysTernaryOperator::Operation<interval_t, T, interval_t, T>);
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				TernaryExecutor::Execute<interval_t, T, interval_t, T>(
				    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
				    TimeBucket::OffsetWidthMoreThanMonthsTernaryOperator::Operation<interval_t, T, interval_t, T>);
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
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
			if (bucket_width.months == 0) {
				int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
				if (bucket_width_micros <= 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				TernaryExecutor::Execute<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucket::OriginWidthLessThanDaysTernaryOperator::Operation<interval_t, T, T, T>);
			} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
				if (bucket_width.months < 0) {
					throw NotImplementedException("Period must be greater than 0");
				}
				TernaryExecutor::Execute<interval_t, T, T, T>(
				    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
				    TimeBucket::OriginWidthMoreThanMonthsTernaryOperator::Operation<interval_t, T, T, T>);
			} else {
				throw NotImplementedException("Month intervals cannot have day or time component");
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
