#include "include/icu-datesub.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/main/extension_util.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

struct ICUCalendarSub : public ICUDateFunc {

	//	ICU only has 32 bit precision for date parts, so it can overflow a high resolution.
	//	Since there is no difference between ICU and the obvious calculations,
	//	we make these using the DuckDB internal type.
	static int64_t SubtractMicrosecond(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		return end_date.value - start_date.value;
	}

	static int64_t SubtractMillisecond(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		return SubtractMicrosecond(calendar, start_date, end_date) / Interval::MICROS_PER_MSEC;
	}

	static int64_t SubtractSecond(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		return SubtractMicrosecond(calendar, start_date, end_date) / Interval::MICROS_PER_SEC;
	}

	static int64_t SubtractMinute(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		return SubtractMicrosecond(calendar, start_date, end_date) / Interval::MICROS_PER_MINUTE;
	}

	static int64_t SubtractHour(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		SetTime(calendar, start_date);
		return SubtractField(calendar, UCAL_HOUR_OF_DAY, end_date);
	}

	static int64_t SubtractDay(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		SetTime(calendar, start_date);
		return SubtractField(calendar, UCAL_DATE, end_date);
	}

	static int64_t SubtractWeek(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		calendar->setFirstDayOfWeek(UCAL_MONDAY);
		calendar->setMinimalDaysInFirstWeek(4);
		SetTime(calendar, start_date);
		return SubtractField(calendar, UCAL_WEEK_OF_YEAR, end_date);
	}

	static int64_t SubtractMonth(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		SetTime(calendar, start_date);
		return SubtractField(calendar, UCAL_MONTH, end_date);
	}

	static int64_t SubtractQuarter(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		// No ICU part for this, so do it manually.
		// This will not work for lunar calendars!
		return SubtractMonth(calendar, start_date, end_date) / 3;
	}

	static int64_t SubtractYear(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		SetTime(calendar, start_date);
		return SubtractField(calendar, UCAL_YEAR, end_date);
	}

	static int64_t SubtractISOYear(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		calendar->setFirstDayOfWeek(UCAL_MONDAY);
		calendar->setMinimalDaysInFirstWeek(4);
		SetTime(calendar, start_date);
		return SubtractField(calendar, UCAL_YEAR_WOY, end_date);
	}

	static int64_t SubtractDecade(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		// No ICU part for this, so do it manually.
		return SubtractYear(calendar, start_date, end_date) / 10;
	}

	static int64_t SubtractCentury(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		// No ICU part for this, so do it manually.
		return SubtractYear(calendar, start_date, end_date) / 100;
	}

	static int64_t SubtractMillenium(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		// No ICU part for this, so do it manually.
		return SubtractYear(calendar, start_date, end_date) / 1000;
	}

	static int64_t SubtractEra(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date) {
		SetTime(calendar, start_date);
		return SubtractField(calendar, UCAL_ERA, end_date);
	}

	template <typename T>
	static void ICUDateSubFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 3);
		auto &part_arg = args.data[0];
		auto &startdate_arg = args.data[1];
		auto &enddate_arg = args.data[2];

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
		CalendarPtr calendar(info.calendar->clone());

		if (part_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// Common case of constant part.
			if (ConstantVector::IsNull(part_arg)) {
				result.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result, true);
			} else {
				const auto specifier = ConstantVector::GetData<string_t>(part_arg)->GetString();
				auto part_func = SubtractFactory(GetDatePartSpecifier(specifier));
				BinaryExecutor::ExecuteWithNulls<T, T, int64_t>(
				    startdate_arg, enddate_arg, result, args.size(),
				    [&](T start_date, T end_date, ValidityMask &mask, idx_t idx) {
					    if (Timestamp::IsFinite(start_date) && Timestamp::IsFinite(end_date)) {
						    return part_func(calendar.get(), start_date, end_date);
					    } else {
						    mask.SetInvalid(idx);
						    return int64_t(0);
					    }
				    });
			}
		} else {
			TernaryExecutor::ExecuteWithNulls<string_t, T, T, int64_t>(
			    part_arg, startdate_arg, enddate_arg, result, args.size(),
			    [&](string_t specifier, T start_date, T end_date, ValidityMask &mask, idx_t idx) {
				    if (Timestamp::IsFinite(start_date) && Timestamp::IsFinite(end_date)) {
					    auto part_func = SubtractFactory(GetDatePartSpecifier(specifier.GetString()));
					    return part_func(calendar.get(), start_date, end_date);
				    } else {
					    mask.SetInvalid(idx);
					    return int64_t(0);
				    }
			    });
		}
	}

	template <typename TA>
	static ScalarFunction GetFunction(const LogicalTypeId &type) {
		return ScalarFunction({LogicalType::VARCHAR, type, type}, LogicalType::BIGINT, ICUDateSubFunction<TA>, Bind);
	}

	static void AddFunctions(const string &name, DatabaseInstance &db) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetFunction<timestamp_t>(LogicalType::TIMESTAMP_TZ));
		ExtensionUtil::AddFunctionOverload(db, set);
	}
};

ICUDateFunc::part_sub_t ICUDateFunc::SubtractFactory(DatePartSpecifier type) {
	switch (type) {
	case DatePartSpecifier::MILLENNIUM:
		return ICUCalendarSub::SubtractMillenium;
	case DatePartSpecifier::CENTURY:
		return ICUCalendarSub::SubtractCentury;
	case DatePartSpecifier::DECADE:
		return ICUCalendarSub::SubtractDecade;
	case DatePartSpecifier::YEAR:
		return ICUCalendarSub::SubtractYear;
	case DatePartSpecifier::QUARTER:
		return ICUCalendarSub::SubtractQuarter;
	case DatePartSpecifier::MONTH:
		return ICUCalendarSub::SubtractMonth;
	case DatePartSpecifier::WEEK:
	case DatePartSpecifier::YEARWEEK:
		return ICUCalendarSub::SubtractWeek;
	case DatePartSpecifier::ISOYEAR:
		return ICUCalendarSub::SubtractISOYear;
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::JULIAN_DAY:
		return ICUCalendarSub::SubtractDay;
	case DatePartSpecifier::HOUR:
		return ICUCalendarSub::SubtractHour;
	case DatePartSpecifier::MINUTE:
		return ICUCalendarSub::SubtractMinute;
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::EPOCH:
		return ICUCalendarSub::SubtractSecond;
	case DatePartSpecifier::MILLISECONDS:
		return ICUCalendarSub::SubtractMillisecond;
	case DatePartSpecifier::MICROSECONDS:
		return ICUCalendarSub::SubtractMicrosecond;
	case DatePartSpecifier::ERA:
		return ICUCalendarSub::SubtractEra;
	default:
		throw NotImplementedException("Specifier type not implemented for ICU subtraction");
	}
}

// MS-SQL differences can be computed using ICU by truncating both arguments
// to the desired part precision and then applying ICU subtraction/difference
struct ICUCalendarDiff : public ICUDateFunc {

	template <typename T>
	static int64_t DifferenceFunc(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date,
	                              part_trunc_t trunc_func, part_sub_t sub_func) {
		// Truncate the two arguments. This is safe because we will stay in range
		auto micros = SetTime(calendar, start_date);
		trunc_func(calendar, micros);
		start_date = GetTimeUnsafe(calendar, micros);

		micros = SetTime(calendar, end_date);
		trunc_func(calendar, micros);
		end_date = GetTimeUnsafe(calendar, micros);

		// Now use ICU difference
		return sub_func(calendar, start_date, end_date);
	}

	static part_trunc_t DiffTruncationFactory(DatePartSpecifier type) {
		switch (type) {
		case DatePartSpecifier::WEEK:
			//	Weeks are computed without anchors
			return TruncationFactory(DatePartSpecifier::DAY);
		default:
			break;
		}
		return TruncationFactory(type);
	}

	template <typename T>
	static void ICUDateDiffFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 3);
		auto &part_arg = args.data[0];
		auto &startdate_arg = args.data[1];
		auto &enddate_arg = args.data[2];

		auto &func_expr = state.expr.Cast<BoundFunctionExpression>();
		auto &info = func_expr.bind_info->Cast<BindData>();
		CalendarPtr calendar_ptr(info.calendar->clone());
		auto calendar = calendar_ptr.get();

		if (part_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// Common case of constant part.
			if (ConstantVector::IsNull(part_arg)) {
				result.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result, true);
			} else {
				const auto specifier = ConstantVector::GetData<string_t>(part_arg)->GetString();
				const auto part = GetDatePartSpecifier(specifier);
				auto trunc_func = DiffTruncationFactory(part);
				auto sub_func = SubtractFactory(part);
				BinaryExecutor::ExecuteWithNulls<T, T, int64_t>(
				    startdate_arg, enddate_arg, result, args.size(),
				    [&](T start_date, T end_date, ValidityMask &mask, idx_t idx) {
					    if (Timestamp::IsFinite(start_date) && Timestamp::IsFinite(end_date)) {
						    return DifferenceFunc<T>(calendar, start_date, end_date, trunc_func, sub_func);
					    } else {
						    mask.SetInvalid(idx);
						    return int64_t(0);
					    }
				    });
			}
		} else {
			TernaryExecutor::ExecuteWithNulls<string_t, T, T, int64_t>(
			    part_arg, startdate_arg, enddate_arg, result, args.size(),
			    [&](string_t specifier, T start_date, T end_date, ValidityMask &mask, idx_t idx) {
				    if (Timestamp::IsFinite(start_date) && Timestamp::IsFinite(end_date)) {
					    const auto part = GetDatePartSpecifier(specifier.GetString());
					    auto trunc_func = DiffTruncationFactory(part);
					    auto sub_func = SubtractFactory(part);
					    return DifferenceFunc<T>(calendar, start_date, end_date, trunc_func, sub_func);
				    } else {
					    mask.SetInvalid(idx);
					    return int64_t(0);
				    }
			    });
		}
	}

	template <typename TA>
	static ScalarFunction GetFunction(const LogicalTypeId &type) {
		return ScalarFunction({LogicalType::VARCHAR, type, type}, LogicalType::BIGINT, ICUDateDiffFunction<TA>, Bind);
	}

	static void AddFunctions(const string &name, DatabaseInstance &db) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetFunction<timestamp_t>(LogicalType::TIMESTAMP_TZ));
		ExtensionUtil::AddFunctionOverload(db, set);
	}
};

void RegisterICUDateSubFunctions(DatabaseInstance &db) {
	ICUCalendarSub::AddFunctions("date_sub", db);
	ICUCalendarSub::AddFunctions("datesub", db);

	ICUCalendarDiff::AddFunctions("date_diff", db);
	ICUCalendarDiff::AddFunctions("datediff", db);
}

} // namespace duckdb
