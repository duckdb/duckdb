#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/operator/subtract.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "include/icu-datefunc.hpp"

namespace duckdb {

struct ICUTimeBucket : public ICUDateFunc {

	// Use 2000-01-03 00:00:00 (Monday) as origin when bucket_width is days, hours, ... for TimescaleDB compatibility
	// There are 10959 days between 1970-01-01 and 2000-01-03
	constexpr static const int64_t DEFAULT_ORIGIN_MICROS_1 = 10959 * Interval::MICROS_PER_DAY;
	// Use 2000-01-01 as origin when bucket_width is months, years, ... for TimescaleDB compatibility
	// There are 10957 days between 1970-01-01 and 2000-01-01
	constexpr static const int64_t DEFAULT_ORIGIN_MICROS_2 = 10957 * Interval::MICROS_PER_DAY;

	enum struct BucketWidthType { CONVERTIBLE_TO_MICROS, CONVERTIBLE_TO_DAYS, CONVERTIBLE_TO_MONTHS, UNCLASSIFIED };

	static inline BucketWidthType ClassifyBucketWidth(const interval_t bucket_width) {
		if (bucket_width.months == 0 && bucket_width.days == 0 && bucket_width.micros > 0) {
			return BucketWidthType::CONVERTIBLE_TO_MICROS;
		} else if (bucket_width.months == 0 && bucket_width.days >= 0 && bucket_width.micros == 0) {
			return BucketWidthType::CONVERTIBLE_TO_DAYS;
		} else if (bucket_width.months > 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
			return BucketWidthType::CONVERTIBLE_TO_MONTHS;
		} else {
			return BucketWidthType::UNCLASSIFIED;
		}
	}

	static inline BucketWidthType ClassifyBucketWidthErrorThrow(const interval_t bucket_width) {
		if (bucket_width.months == 0 && bucket_width.days == 0) {
			if (bucket_width.micros <= 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return BucketWidthType::CONVERTIBLE_TO_MICROS;
		} else if (bucket_width.months == 0 && bucket_width.micros == 0) {
			if (bucket_width.days <= 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return BucketWidthType::CONVERTIBLE_TO_DAYS;
		} else if (bucket_width.days == 0 && bucket_width.micros == 0) {
			if (bucket_width.months <= 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return BucketWidthType::CONVERTIBLE_TO_MONTHS;
		} else if (bucket_width.months == 0) {
			throw NotImplementedException("Day intervals cannot have time component");
		} else {
			throw NotImplementedException("Month intervals cannot have day or time component");
		}
	}

	static inline timestamp_t WidthConvertibleToMicrosCommon(int64_t bucket_width_micros, const timestamp_t ts,
	                                                         const timestamp_t origin, icu::Calendar *calendar) {
		int64_t ts_micros = SubtractOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(
		    Timestamp::GetEpochMicroSeconds(ts), Timestamp::GetEpochMicroSeconds(origin));
		int64_t result_micros = (ts_micros / bucket_width_micros) * bucket_width_micros;
		if (ts_micros < 0 && ts_micros % bucket_width_micros != 0) {
			result_micros =
			    SubtractOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(result_micros, bucket_width_micros);
		}

		return Add(calendar, origin, interval_t {0, 0, result_micros});
	}

	static inline timestamp_t WidthConvertibleToDaysCommon(int32_t bucket_width_days, const timestamp_t ts,
	                                                       const timestamp_t origin, icu::Calendar *calendar) {
		const auto trunc_days = TruncationFactory(DatePartSpecifier::DAY);
		const auto sub_days = SubtractFactory(DatePartSpecifier::DAY);

		uint64_t tmp_micros = SetTime(calendar, ts);
		trunc_days(calendar, tmp_micros);
		timestamp_t truncated_ts = GetTimeUnsafe(calendar, tmp_micros);

		int64_t ts_days = sub_days(calendar, origin, truncated_ts);
		int64_t result_days = (ts_days / bucket_width_days) * bucket_width_days;
		if (result_days < NumericLimits<int32_t>::Minimum() || result_days > NumericLimits<int32_t>::Maximum()) {
			throw OutOfRangeException("Timestamp out of range");
		}
		if (ts_days < 0 && ts_days % bucket_width_days != 0) {
			result_days =
			    SubtractOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(result_days, bucket_width_days);
		}

		return Add(calendar, origin, interval_t {0, static_cast<int32_t>(result_days), 0});
	}

	static inline timestamp_t WidthConvertibleToMonthsCommon(int32_t bucket_width_months, const timestamp_t ts,
	                                                         const timestamp_t origin, icu::Calendar *calendar) {
		const auto trunc_months = TruncationFactory(DatePartSpecifier::MONTH);
		const auto sub_months = SubtractFactory(DatePartSpecifier::MONTH);

		uint64_t tmp_micros = SetTime(calendar, ts);
		trunc_months(calendar, tmp_micros);
		timestamp_t truncated_ts = GetTimeUnsafe(calendar, tmp_micros);

		tmp_micros = SetTime(calendar, origin);
		trunc_months(calendar, tmp_micros);
		timestamp_t truncated_origin = GetTimeUnsafe(calendar, tmp_micros);

		int64_t ts_months = sub_months(calendar, truncated_origin, truncated_ts);
		int64_t result_months = (ts_months / bucket_width_months) * bucket_width_months;
		if (result_months < NumericLimits<int32_t>::Minimum() || result_months > NumericLimits<int32_t>::Maximum()) {
			throw OutOfRangeException("Timestamp out of range");
		}
		if (ts_months < 0 && ts_months % bucket_width_months != 0) {
			result_months =
			    SubtractOperatorOverflowCheck::Operation<int32_t, int32_t, int32_t>(result_months, bucket_width_months);
		}

		return Add(calendar, truncated_origin, interval_t {static_cast<int32_t>(result_months), 0, 0});
	}

	template <typename TA, typename TB, typename TR, typename OP>
	static void ExecuteBinary(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
		CalendarPtr calendar(info.calendar->clone());

		BinaryExecutor::Execute<TA, TB, TR>(args.data[0], args.data[1], result, args.size(), [&](TA left, TB right) {
			return OP::template Operation<TA, TB, TR>(left, right, calendar);
		});
	}

	template <typename TA, typename TB, typename TC, typename TR, typename OP>
	static void ExecuteTernary(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 3);

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
		CalendarPtr calendar(info.calendar->clone());

		TernaryExecutor::Execute<TA, TB, TC, TR>(
		    args.data[0], args.data[1], args.data[2], result, args.size(), [&](TA ta, TB tb, TC tc) {
			    return OP::template Operation<TA, TB, TC, TR>(args.data[0], args.data[1], args.data[2], calendar.get());
		    });
	}

	struct WidthConvertibleToMicrosBinaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			const auto origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_1);
			return WidthConvertibleToMicrosCommon(bucket_width.micros, ts, origin, calendar);
		}
	};

	struct WidthConvertibleToDaysBinaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			const auto origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_1);
			return WidthConvertibleToDaysCommon(bucket_width.days, ts, origin, calendar);
		}
	};

	struct WidthConvertibleToMonthsBinaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			const auto origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_2);
			return WidthConvertibleToMonthsCommon(bucket_width.months, ts, origin, calendar);
		}
	};

	struct BinaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, icu::Calendar *calendar) {
			BucketWidthType bucket_width_type = ClassifyBucketWidthErrorThrow(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::CONVERTIBLE_TO_MICROS:
				return WidthConvertibleToMicrosBinaryOperator::Operation(bucket_width, ts, calendar);
			case BucketWidthType::CONVERTIBLE_TO_DAYS:
				return WidthConvertibleToDaysBinaryOperator::Operation(bucket_width, ts, calendar);
			case BucketWidthType::CONVERTIBLE_TO_MONTHS:
				return WidthConvertibleToMonthsBinaryOperator::Operation(bucket_width, ts, calendar);
			default:
				throw NotImplementedException("Bucket type not implemented for ICU TIME_BUCKET");
			}
		}
	};

	struct OffsetWidthConvertibleToMicrosTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, interval_t offset,
		                                    icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			const auto origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_1);
			return Add(calendar,
			           WidthConvertibleToMicrosCommon(bucket_width.micros, Sub(calendar, ts, offset), origin, calendar),
			           offset);
		}
	};

	struct OffsetWidthConvertibleToDaysTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, interval_t offset,
		                                    icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			const auto origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_1);
			return Add(calendar,
			           WidthConvertibleToDaysCommon(bucket_width.days, Sub(calendar, ts, offset), origin, calendar),
			           offset);
		}
	};

	struct OffsetWidthConvertibleToMonthsTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, interval_t offset,
		                                    icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			const auto origin = Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_2);
			return Add(calendar,
			           WidthConvertibleToMonthsCommon(bucket_width.months, Sub(calendar, ts, offset), origin, calendar),
			           offset);
		}
	};

	struct OffsetTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, interval_t offset,
		                                    icu::Calendar *calendar) {
			BucketWidthType bucket_width_type = ClassifyBucketWidthErrorThrow(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::CONVERTIBLE_TO_MICROS:
				return OffsetWidthConvertibleToMicrosTernaryOperator::Operation(bucket_width, ts, offset, calendar);
			case BucketWidthType::CONVERTIBLE_TO_DAYS:
				return OffsetWidthConvertibleToDaysTernaryOperator::Operation(bucket_width, ts, offset, calendar);
			case BucketWidthType::CONVERTIBLE_TO_MONTHS:
				return OffsetWidthConvertibleToMonthsTernaryOperator::Operation(bucket_width, ts, offset, calendar);
			default:
				throw NotImplementedException("Bucket type not implemented for ICU TIME_BUCKET");
			}
		}
	};

	struct OriginWidthConvertibleToMicrosTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, timestamp_t origin,
		                                    icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			return WidthConvertibleToMicrosCommon(bucket_width.micros, ts, origin, calendar);
		}
	};

	struct OriginWidthConvertibleToDaysTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, timestamp_t origin,
		                                    icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			return WidthConvertibleToDaysCommon(bucket_width.days, ts, origin, calendar);
		}
	};

	struct OriginWidthConvertibleToMonthsTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, timestamp_t origin,
		                                    icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			return WidthConvertibleToMonthsCommon(bucket_width.months, ts, origin, calendar);
		}
	};

	struct OriginTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, timestamp_t origin,
		                                    ValidityMask &mask, idx_t idx, icu::Calendar *calendar) {
			if (!Value::IsFinite(origin)) {
				mask.SetInvalid(idx);
				return timestamp_t(0);
			}
			BucketWidthType bucket_width_type = ClassifyBucketWidthErrorThrow(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::CONVERTIBLE_TO_MICROS:
				return OriginWidthConvertibleToMicrosTernaryOperator::Operation(bucket_width, ts, origin, calendar);
			case BucketWidthType::CONVERTIBLE_TO_DAYS:
				return OriginWidthConvertibleToDaysTernaryOperator::Operation(bucket_width, ts, origin, calendar);
			case BucketWidthType::CONVERTIBLE_TO_MONTHS:
				return OriginWidthConvertibleToMonthsTernaryOperator::Operation(bucket_width, ts, origin, calendar);
			default:
				throw NotImplementedException("Bucket type not implemented for ICU TIME_BUCKET");
			}
		}
	};

	struct TimeZoneWidthConvertibleToMicrosBinaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, timestamp_t origin,
		                                    icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			return WidthConvertibleToMicrosCommon(bucket_width.micros, ts, origin, calendar);
		}
	};

	struct TimeZoneWidthConvertibleToDaysBinaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, timestamp_t origin,
		                                    icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			return WidthConvertibleToDaysCommon(bucket_width.days, ts, origin, calendar);
		}
	};

	struct TimeZoneWidthConvertibleToMonthsBinaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, timestamp_t origin,
		                                    icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			return WidthConvertibleToMonthsCommon(bucket_width.months, ts, origin, calendar);
		}
	};

	struct TimeZoneTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, string_t tz,
		                                    icu::Calendar *calendar) {
			SetTimeZone(calendar, tz);

			timestamp_t origin;
			BucketWidthType bucket_width_type = ClassifyBucketWidthErrorThrow(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::CONVERTIBLE_TO_MICROS:
				origin = ICUDateFunc::FromNaive(calendar, Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_1));
				return TimeZoneWidthConvertibleToMicrosBinaryOperator::Operation(bucket_width, ts, origin, calendar);
			case BucketWidthType::CONVERTIBLE_TO_DAYS:
				origin = ICUDateFunc::FromNaive(calendar, Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_1));
				return TimeZoneWidthConvertibleToDaysBinaryOperator::Operation(bucket_width, ts, origin, calendar);
			case BucketWidthType::CONVERTIBLE_TO_MONTHS:
				origin = ICUDateFunc::FromNaive(calendar, Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_2));
				return TimeZoneWidthConvertibleToMonthsBinaryOperator::Operation(bucket_width, ts, origin, calendar);
			default:
				throw NotImplementedException("Bucket type not implemented for ICU TIME_BUCKET");
			}
		}
	};

	static void ICUTimeBucketFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
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
				BucketWidthType bucket_width_type = ClassifyBucketWidth(bucket_width);
				switch (bucket_width_type) {
				case BucketWidthType::CONVERTIBLE_TO_MICROS:
					BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
						    return WidthConvertibleToMicrosBinaryOperator::Operation(bucket_width, ts, calendar);
					    });
					break;
				case BucketWidthType::CONVERTIBLE_TO_DAYS:
					BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
						    return WidthConvertibleToDaysBinaryOperator::Operation(bucket_width, ts, calendar);
					    });
					break;
				case BucketWidthType::CONVERTIBLE_TO_MONTHS:
					BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
						    return WidthConvertibleToMonthsBinaryOperator::Operation(bucket_width, ts, calendar);
					    });
					break;
				case BucketWidthType::UNCLASSIFIED:
					BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
						    return BinaryOperator::Operation(bucket_width, ts, calendar);
					    });
					break;
				default:
					throw NotImplementedException("Bucket type not implemented for ICU TIME_BUCKET");
				}
			}
		} else {
			BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
			    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
				    return BinaryOperator::Operation(bucket_width, ts, calendar);
			    });
		}
	}

	static void ICUTimeBucketOffsetFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 3);

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
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
				BucketWidthType bucket_width_type = ClassifyBucketWidth(bucket_width);
				switch (bucket_width_type) {
				case BucketWidthType::CONVERTIBLE_TO_MICROS:
					TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
					    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
						    return OffsetWidthConvertibleToMicrosTernaryOperator::Operation(bucket_width, ts, offset,
						                                                                    calendar);
					    });
					break;
				case BucketWidthType::CONVERTIBLE_TO_DAYS:
					TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
					    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
						    return OffsetWidthConvertibleToDaysTernaryOperator::Operation(bucket_width, ts, offset,
						                                                                  calendar);
					    });
					break;
				case BucketWidthType::CONVERTIBLE_TO_MONTHS:
					TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
					    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
						    return OffsetWidthConvertibleToMonthsTernaryOperator::Operation(bucket_width, ts, offset,
						                                                                    calendar);
					    });
					break;
				case BucketWidthType::UNCLASSIFIED:
					TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
					    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
						    return OffsetTernaryOperator::Operation(bucket_width, ts, offset, calendar);
					    });
					break;
				default:
					throw NotImplementedException("Bucket type not implemented for ICU TIME_BUCKET");
				}
			}
		} else {
			TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
			    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
			    [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
				    return OffsetTernaryOperator::Operation(bucket_width, ts, offset, calendar);
			    });
		}
	}

	static void ICUTimeBucketOriginFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 3);

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
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
				BucketWidthType bucket_width_type = ClassifyBucketWidth(bucket_width);
				switch (bucket_width_type) {
				case BucketWidthType::CONVERTIBLE_TO_MICROS:
					TernaryExecutor::Execute<interval_t, timestamp_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin) {
						    return OriginWidthConvertibleToMicrosTernaryOperator::Operation(bucket_width, ts, origin,
						                                                                    calendar);
					    });
					break;
				case BucketWidthType::CONVERTIBLE_TO_DAYS:
					TernaryExecutor::Execute<interval_t, timestamp_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin) {
						    return OriginWidthConvertibleToDaysTernaryOperator::Operation(bucket_width, ts, origin,
						                                                                  calendar);
					    });
					break;
				case BucketWidthType::CONVERTIBLE_TO_MONTHS:
					TernaryExecutor::Execute<interval_t, timestamp_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin) {
						    return OriginWidthConvertibleToMonthsTernaryOperator::Operation(bucket_width, ts, origin,
						                                                                    calendar);
					    });
					break;
				case BucketWidthType::UNCLASSIFIED:
					TernaryExecutor::ExecuteWithNulls<interval_t, timestamp_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin, ValidityMask &mask,
					        idx_t idx) {
						    return OriginTernaryOperator::Operation(bucket_width, ts, origin, mask, idx, calendar);
					    });
					break;
				default:
					throw NotImplementedException("Bucket type not implemented for ICU TIME_BUCKET");
				}
			}
		} else {
			TernaryExecutor::ExecuteWithNulls<interval_t, timestamp_t, timestamp_t, timestamp_t>(
			    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
			    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin, ValidityMask &mask, idx_t idx) {
				    return OriginTernaryOperator::Operation(bucket_width, ts, origin, mask, idx, calendar);
			    });
		}
	}

	static void ICUTimeBucketTimeZoneFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 3);

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
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
				timestamp_t origin;
				BucketWidthType bucket_width_type = ClassifyBucketWidth(bucket_width);
				switch (bucket_width_type) {
				case BucketWidthType::CONVERTIBLE_TO_MICROS:
					origin =
					    ICUDateFunc::FromNaive(calendar, Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_1));
					BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
						    return TimeZoneWidthConvertibleToMicrosBinaryOperator::Operation(bucket_width, ts, origin,
						                                                                     calendar);
					    });
					break;
				case BucketWidthType::CONVERTIBLE_TO_DAYS:
					origin =
					    ICUDateFunc::FromNaive(calendar, Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_1));
					BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
						    return TimeZoneWidthConvertibleToDaysBinaryOperator::Operation(bucket_width, ts, origin,
						                                                                   calendar);
					    });
					break;
				case BucketWidthType::CONVERTIBLE_TO_MONTHS:
					origin =
					    ICUDateFunc::FromNaive(calendar, Timestamp::FromEpochMicroSeconds(DEFAULT_ORIGIN_MICROS_2));
					BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
						    return TimeZoneWidthConvertibleToMonthsBinaryOperator::Operation(bucket_width, ts, origin,
						                                                                     calendar);
					    });
					break;
				case BucketWidthType::UNCLASSIFIED:
					TernaryExecutor::Execute<interval_t, timestamp_t, string_t, timestamp_t>(
					    bucket_width_arg, ts_arg, tz_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, string_t tz) {
						    return TimeZoneTernaryOperator::Operation(bucket_width, ts, tz, calendar);
					    });
					break;
				default:
					throw NotImplementedException("Bucket type not implemented for ICU TIME_BUCKET");
				}
			}
		} else {
			TernaryExecutor::Execute<interval_t, timestamp_t, string_t, timestamp_t>(
			    bucket_width_arg, ts_arg, tz_arg, result, args.size(),
			    [&](interval_t bucket_width, timestamp_t ts, string_t tz) {
				    return TimeZoneTernaryOperator::Operation(bucket_width, ts, tz, calendar);
			    });
		}
	}

	static void AddTimeBucketFunction(ClientContext &context) {
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
		auto &catalog = Catalog::GetSystemCatalog(context);
		catalog.AddFunction(context, func_info);
	}
};

void RegisterICUTimeBucketFunctions(ClientContext &context) {
	ICUTimeBucket::AddTimeBucketFunction(context);
}

} // namespace duckdb
