#include "duckdb/common/exception.hpp"
#include "duckdb/common/limits.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/value.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/common/vector_operations/ternary_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "include/icu-dateadd.hpp"
#include "include/icu-datefunc.hpp"
#include "include/icu-timezone.hpp"

namespace duckdb {

struct ICUTimeBucket : public ICUDateFunc {

	// Use 2000-01-03 00:00:00 (Monday) as origin when bucket_width is days, hours, ... for TimescaleDB compatibility
	// There are 10959 days between 1970-01-01 and 2000-01-03
	constexpr static const int64_t DEFAULT_ORIGIN_MICROS = 10959 * Interval::MICROS_PER_DAY;
	// Use 2000-01-01 as origin when bucket_width is months, years, ... for TimescaleDB compatibility
	// There are 360 months between 1970-01-01 and 2000-01-01
	constexpr static const int64_t DEFAULT_ORIGIN_MONTHS = 360;

	enum struct BucketWidthType { LESS_THAN_DAYS, MORE_THAN_MONTHS, UNCLASSIFIED };

	static inline BucketWidthType ClassifyBucketWidth(const interval_t bucket_width) {
		if (bucket_width.months == 0 && Interval::GetMicro(bucket_width) > 0) {
			return BucketWidthType::LESS_THAN_DAYS;
		} else if (bucket_width.months > 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
			return BucketWidthType::MORE_THAN_MONTHS;
		} else {
			return BucketWidthType::UNCLASSIFIED;
		}
	}

	static inline BucketWidthType ClassifyBucketWidthWithThrowingError(const interval_t bucket_width) {
		if (bucket_width.months == 0) {
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			if (bucket_width_micros <= 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return BucketWidthType::LESS_THAN_DAYS;
		} else if (bucket_width.months != 0 && bucket_width.days == 0 && bucket_width.micros == 0) {
			if (bucket_width.months < 0) {
				throw NotImplementedException("Period must be greater than 0");
			}
			return BucketWidthType::MORE_THAN_MONTHS;
		} else {
			throw NotImplementedException("Month intervals cannot have day or time component");
		}
	}

	static inline int32_t EpochMonths(timestamp_t ts, icu::Calendar *calendar) {
		SetTime(calendar, ts);
		return (ExtractField(calendar, UCAL_YEAR) - 1970) * 12 + ExtractField(calendar, UCAL_MONTH);
	}

	static inline timestamp_t WidthLessThanDaysCommon(int64_t bucket_width_micros, int64_t ts_micros,
	                                                  int64_t origin_micros, icu::Calendar *calendar) {
		origin_micros %= bucket_width_micros;
		if (origin_micros > 0 && ts_micros < NumericLimits<int64_t>::Minimum() + origin_micros) {
			throw OutOfRangeException("Timestamp out of range");
		}
		if (origin_micros < 0 && ts_micros > NumericLimits<int64_t>::Maximum() + origin_micros) {
			throw OutOfRangeException("Timestamp out of range");
		}
		ts_micros -= origin_micros;

		int64_t result_micros = (ts_micros / bucket_width_micros) * bucket_width_micros;
		if (ts_micros < 0 && ts_micros % bucket_width_micros != 0) {
			if (result_micros < NumericLimits<int64_t>::Minimum() + bucket_width_micros) {
				throw OutOfRangeException("Timestamp out of range");
			}
			result_micros -= bucket_width_micros;
		}
		result_micros += origin_micros;

		return Add(calendar, timestamp_t::epoch(), Interval::FromMicro(result_micros));
	}

	static inline timestamp_t WidthMoreThanMonthsCommon(int32_t bucket_width_months, int32_t ts_months,
	                                                    int32_t origin_months, icu::Calendar *calendar) {
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
			result_months -= bucket_width_months;
		}
		result_months += origin_months;

		int32_t year =
		    (result_months < 0 && result_months % 12 != 0) ? 1970 + result_months / 12 - 1 : 1970 + result_months / 12;
		int32_t month =
		    (result_months < 0 && result_months % 12 != 0) ? result_months % 12 + 13 : result_months % 12 + 1;

		return Cast::template Operation<date_t, timestamp_t>(Date::FromDate(year, month, 1));
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
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(ts);
			return WidthLessThanDaysCommon(bucket_width_micros, ts_micros, DEFAULT_ORIGIN_MICROS, calendar);
		}
	};

	struct WidthMoreThanMonthsBinaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			int32_t ts_months = EpochMonths(ts, calendar);
			return WidthMoreThanMonthsCommon(bucket_width.months, ts_months, DEFAULT_ORIGIN_MONTHS, calendar);
		}
	};

	struct BinaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, icu::Calendar *calendar) {
			BucketWidthType bucket_width_type = ClassifyBucketWidthWithThrowingError(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::LESS_THAN_DAYS:
				return WidthLessThanDaysBinaryOperator::Operation(bucket_width, ts, calendar);
			default:
				return WidthMoreThanMonthsBinaryOperator::Operation(bucket_width, ts, calendar);
			}
		}
	};

	struct OffsetWidthLessThanDaysTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, interval_t offset,
			                                   icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(Sub(calendar, ts, offset));
			return Add(calendar,
				           WidthLessThanDaysCommon(bucket_width_micros, ts_micros, DEFAULT_ORIGIN_MICROS, calendar),
				           offset);
		}
	};

	struct OffsetWidthMoreThanMonthsTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, interval_t offset,
			                                icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			int32_t ts_months = EpochMonths(Sub(calendar, ts, offset), calendar);
			return Add(calendar,
				       WidthMoreThanMonthsCommon(bucket_width.months, ts_months, DEFAULT_ORIGIN_MONTHS, calendar),
				       offset);
		}
	};

	struct OffsetTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, interval_t offset,
		                           icu::Calendar *calendar) {
			BucketWidthType bucket_width_type = ClassifyBucketWidthWithThrowingError(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::LESS_THAN_DAYS:
				return OffsetWidthLessThanDaysTernaryOperator::Operation(bucket_width, ts, offset, calendar);
			default:
				return OffsetWidthMoreThanMonthsTernaryOperator::Operation(bucket_width, ts, offset, calendar);
			}
		}
	};

	struct OriginWidthLessThanDaysTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, timestamp_t origin,
		                           icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(ts);
			int64_t origin_micros = Timestamp::GetEpochMicroSeconds(origin);
			return WidthLessThanDaysCommon(bucket_width_micros, ts_micros, origin_micros, calendar);
		}
	};

	struct OriginWidthMoreThanMonthsTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, timestamp_t origin,
		                           icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			int32_t ts_months = EpochMonths(ts, calendar);
			int32_t origin_months = EpochMonths(origin, calendar);
			return WidthMoreThanMonthsCommon(bucket_width.months, ts_months, origin_months, calendar);
		}
	};

	struct OriginTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, timestamp_t origin, ValidityMask &mask,
		                           idx_t idx, icu::Calendar *calendar) {
			if (!Value::IsFinite(origin)) {
				mask.SetInvalid(idx);
				return timestamp_t(0);
			}
			BucketWidthType bucket_width_type = ClassifyBucketWidthWithThrowingError(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::LESS_THAN_DAYS:
				return OriginWidthLessThanDaysTernaryOperator::Operation(bucket_width, ts, origin, calendar);
			default:
				return OriginWidthMoreThanMonthsTernaryOperator::Operation(bucket_width, ts, origin, calendar);
			}
		}
	};

	struct TimeZoneWidthLessThanDaysBinaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			int64_t bucket_width_micros = Interval::GetMicro(bucket_width);
			int64_t ts_micros = Timestamp::GetEpochMicroSeconds(ts);
			return WidthLessThanDaysCommon(bucket_width_micros, ts_micros, DEFAULT_ORIGIN_MICROS, calendar);
		}
	};

	struct TimeZoneWidthMoreThanMonthsBinaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, icu::Calendar *calendar) {
			if (!Value::IsFinite(ts)) {
				return ts;
			}
			int32_t ts_months = EpochMonths(ts, calendar);
			return WidthMoreThanMonthsCommon(bucket_width.months, ts_months, DEFAULT_ORIGIN_MONTHS, calendar);
		}
	};

	struct TimeZoneTernaryOperator {
		static inline timestamp_t Operation(interval_t bucket_width, timestamp_t ts, string_t tz, icu::Calendar *calendar) {
			SetTimeZone(calendar, tz);
			BucketWidthType bucket_width_type = ClassifyBucketWidthWithThrowingError(bucket_width);
			switch (bucket_width_type) {
			case BucketWidthType::LESS_THAN_DAYS:
				return TimeZoneWidthLessThanDaysBinaryOperator::Operation(bucket_width, ts, calendar);
			default:
				return TimeZoneWidthMoreThanMonthsBinaryOperator::Operation(bucket_width, ts, calendar);
			}
		}
	};

	static void ICUTimeBucketFunction(DataChunk &args, ExpressionState &state, Vector &result) {
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
				BucketWidthType bucket_width_type = ClassifyBucketWidth(bucket_width);
				switch (bucket_width_type) {
				case BucketWidthType::LESS_THAN_DAYS:
					BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
						    return WidthLessThanDaysBinaryOperator::Operation(bucket_width, ts, calendar);
					    });
					break;
				case BucketWidthType::MORE_THAN_MONTHS:
					BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
						    return WidthMoreThanMonthsBinaryOperator::Operation(bucket_width, ts, calendar);
					    });
					break;
				default:
					BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
						    return BinaryOperator::Operation(bucket_width, ts, calendar);
					    });
					break;
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
				BucketWidthType bucket_width_type = ClassifyBucketWidth(bucket_width);
				switch (bucket_width_type) {
				case BucketWidthType::LESS_THAN_DAYS:
					TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
					    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
						    return OffsetWidthLessThanDaysTernaryOperator::Operation(bucket_width, ts, offset,
						                                                             calendar);
					    });
					break;
				case BucketWidthType::MORE_THAN_MONTHS:
					TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
					    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
						    return OffsetWidthMoreThanMonthsTernaryOperator::Operation(bucket_width, ts, offset,
						                                                               calendar);
					    });
					break;
				default:
					TernaryExecutor::Execute<interval_t, timestamp_t, interval_t, timestamp_t>(
					    bucket_width_arg, ts_arg, offset_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, interval_t offset) {
						    return OffsetTernaryOperator::Operation(bucket_width, ts, offset, calendar);
					    });
					break;
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
				BucketWidthType bucket_width_type = ClassifyBucketWidth(bucket_width);
				switch (bucket_width_type) {
				case BucketWidthType::LESS_THAN_DAYS:
					TernaryExecutor::Execute<interval_t, timestamp_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin) {
						    return OriginWidthLessThanDaysTernaryOperator::Operation(bucket_width, ts, origin,
						                                                             calendar);
					    });
					break;
				case BucketWidthType::MORE_THAN_MONTHS:
					TernaryExecutor::Execute<interval_t, timestamp_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin) {
						    return OriginWidthMoreThanMonthsTernaryOperator::Operation(bucket_width, ts, origin,
						                                                               calendar);
					    });
					break;
				default:
					TernaryExecutor::ExecuteWithNulls<interval_t, timestamp_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, origin_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, timestamp_t origin, ValidityMask &mask,
					        idx_t idx) {
						    return OriginTernaryOperator::Operation(bucket_width, ts, origin, mask, idx, calendar);
					    });
					break;
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
				case BucketWidthType::LESS_THAN_DAYS:
					BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
						    return TimeZoneWidthLessThanDaysBinaryOperator::Operation(bucket_width, ts, calendar);
					    });
					break;
				case BucketWidthType::MORE_THAN_MONTHS:
					BinaryExecutor::Execute<interval_t, timestamp_t, timestamp_t>(
					    bucket_width_arg, ts_arg, result, args.size(), [&](interval_t bucket_width, timestamp_t ts) {
						    return TimeZoneWidthMoreThanMonthsBinaryOperator::Operation(bucket_width, ts, calendar);
					    });
					break;
				default:
					TernaryExecutor::Execute<interval_t, timestamp_t, string_t, timestamp_t>(
					    bucket_width_arg, ts_arg, tz_arg, result, args.size(),
					    [&](interval_t bucket_width, timestamp_t ts, string_t tz) {
						    return TimeZoneTernaryOperator::Operation(bucket_width, ts, tz, calendar);
					    });
					break;
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

	static void AddTimeBucketFunction(const string &name, ClientContext &context) {
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
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUTimeBucketFunctions(ClientContext &context) {
	ICUTimeBucket::AddTimeBucketFunction("time_bucket", context);
}

} // namespace duckdb
