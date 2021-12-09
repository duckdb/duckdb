#include "include/icu-datesub.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"

namespace duckdb {

struct ICUCalendarSub : public ICUDateFunc {
	typedef int64_t (*part_sub_t)(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date);

	static int64_t SubtractField(icu::Calendar *calendar, UCalendarDateFields field, timestamp_t end_date) {
		// ICU triggers the address sanitiser because it tries to left shift a negative value
		// when start_date > end_date. To avoid this, we swap the values and negate the result.
		const auto start_date = GetTimeUnsafe(calendar);
		if (start_date > end_date) {
			SetTime(calendar, end_date);
			return -SubtractField(calendar, field, start_date);
		}

		const int64_t millis = end_date.value / Interval::MICROS_PER_MSEC;
		const auto when = UDate(millis);
		UErrorCode status = U_ZERO_ERROR;
		auto sub = calendar->fieldDifference(when, field, status);
		if (U_FAILURE(status)) {
			throw Exception("Unable to subtract ICU calendar part.");
		}
		return sub;
	}

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

	static part_sub_t SubtractFactory(DatePartSpecifier type) {
		switch (type) {
		case DatePartSpecifier::MILLENNIUM:
			return SubtractMillenium;
		case DatePartSpecifier::CENTURY:
			return SubtractCentury;
		case DatePartSpecifier::DECADE:
			return SubtractDecade;
		case DatePartSpecifier::YEAR:
			return SubtractYear;
		case DatePartSpecifier::QUARTER:
			return SubtractQuarter;
		case DatePartSpecifier::MONTH:
			return SubtractMonth;
		case DatePartSpecifier::WEEK:
		case DatePartSpecifier::YEARWEEK:
			return SubtractWeek;
		case DatePartSpecifier::DAY:
		case DatePartSpecifier::DOW:
		case DatePartSpecifier::ISODOW:
		case DatePartSpecifier::DOY:
			return SubtractDay;
		case DatePartSpecifier::HOUR:
			return SubtractHour;
		case DatePartSpecifier::MINUTE:
			return SubtractMinute;
		case DatePartSpecifier::SECOND:
		case DatePartSpecifier::EPOCH:
			return SubtractSecond;
		case DatePartSpecifier::MILLISECONDS:
			return SubtractMillisecond;
		case DatePartSpecifier::MICROSECONDS:
			return SubtractMicrosecond;
		default:
			throw NotImplementedException("Specifier type not implemented for DATEDIFF");
		}
	}

	template <typename T>
	static void ICUDateSubFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 3);
		auto &part_arg = args.data[0];
		auto &startdate_arg = args.data[1];
		auto &enddate_arg = args.data[2];

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		if (part_arg.GetVectorType() == VectorType::CONSTANT_VECTOR) {
			// Common case of constant part.
			if (ConstantVector::IsNull(part_arg)) {
				result.SetVectorType(VectorType::CONSTANT_VECTOR);
				ConstantVector::SetNull(result, true);
			} else {
				const auto specifier = ConstantVector::GetData<string_t>(part_arg)->GetString();
				auto part_func = SubtractFactory(GetDatePartSpecifier(specifier));
				BinaryExecutor::Execute<T, T, int64_t>(
				    startdate_arg, enddate_arg, result, args.size(),
				    [&](T start_date, T end_date) { return part_func(calendar.get(), start_date, end_date); });
			}
		} else {
			TernaryExecutor::Execute<string_t, T, T, int64_t>(
			    part_arg, startdate_arg, enddate_arg, result, args.size(),
			    [&](string_t specifier, T start_date, T end_date) {
				    auto part_func = SubtractFactory(GetDatePartSpecifier(specifier.GetString()));
				    return part_func(calendar.get(), start_date, end_date);
			    });
		}
	}

	template <typename TA>
	static ScalarFunction GetDateSubtractFunction(const LogicalTypeId &type) {
		return ScalarFunction({LogicalType::VARCHAR, type, type}, LogicalType::BIGINT, ICUDateSubFunction<TA>, false,
		                      Bind);
	}

	static void AddDateSubFunctions(const string &name, ClientContext &context) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetDateSubtractFunction<timestamp_t>(LogicalType::TIMESTAMP_TZ));

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUDateSubFunctions(ClientContext &context) {
	ICUCalendarSub::AddDateSubFunctions("date_sub", context);
	ICUCalendarSub::AddDateSubFunctions("datesub", context);
}

} // namespace duckdb
