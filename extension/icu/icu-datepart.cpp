#include "icu-datepart.hpp"
#include "icu-collate.hpp"

#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ICUDatePart {
	using CalendarPtr = unique_ptr<icu::Calendar>;
	typedef int32_t (*PartAdapter)(icu::Calendar *calendar, const uint64_t micros);

	static DatePartSpecifier PartCodeFromFunction(const string &name) {
		return GetDatePartSpecifier(name.substr(4));
	}

	static int32_t ExtractField(icu::Calendar *calendar, UCalendarDateFields field) {
		UErrorCode status = U_ZERO_ERROR;
		const auto result = calendar->get(field, status);
		if (U_FAILURE(status)) {
			throw Exception("Unable to extract ICU date part.");
		}
		return result;
	}

	// Date part adapters
	static int32_t ExtractYear(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_YEAR);
	}

	static int32_t ExtractDecade(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractYear(calendar, micros) / 10;
	}

	static int32_t ExtractCentury(icu::Calendar *calendar, const uint64_t micros) {
		return 1 + ExtractYear(calendar, micros) / 100;
	}

	static int32_t ExtractMillenium(icu::Calendar *calendar, const uint64_t micros) {
		return 1 + ExtractYear(calendar, micros) / 1000;
	}

	static int32_t ExtractMonth(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_MONTH) + 1;
	}

	static int32_t ExtractQuarter(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_MONTH) / Interval::MONTHS_PER_QUARTER + 1;
	}

	static int32_t ExtractDay(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_DATE);
	}

	static int32_t ExtractDayOfWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_SUNDAY);
		return ExtractField(calendar, UCAL_DAY_OF_WEEK) - UCAL_SUNDAY;
	}

	static int32_t ExtractISODayOfWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_MONDAY);
		return ExtractField(calendar, UCAL_DAY_OF_WEEK);
	}

	static int32_t ExtractWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_SUNDAY);
		calendar->setMinimalDaysInFirstWeek(4);
		return ExtractField(calendar, UCAL_WEEK_OF_YEAR);
	}

	static int32_t ExtractYearWeek(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractYear(calendar, micros) * 100 + ExtractWeek(calendar, micros);
	}

	static int32_t ExtractDayOfYear(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_DAY_OF_YEAR);
	}

	static int32_t ExtractHour(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_HOUR_OF_DAY);
	}

	static int32_t ExtractMinute(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_MINUTE);
	}

	static int32_t ExtractSecond(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_SECOND);
	}

	static int32_t ExtractMillisecond(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractSecond(calendar, micros) * Interval::MSECS_PER_SEC + ExtractField(calendar, UCAL_MILLISECOND);
	}

	static int32_t ExtractMicrosecond(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractMillisecond(calendar, micros) * Interval::MICROS_PER_MSEC + micros;
	}

	static int32_t ExtractEpoch(icu::Calendar *calendar, const uint64_t micros) {
		UErrorCode status = U_ZERO_ERROR;
		auto millis = calendar->getTime(status);
		millis -= ExtractField(calendar, UCAL_ZONE_OFFSET);
		millis -= ExtractField(calendar, UCAL_DST_OFFSET);
		//	Truncate
		return int32_t(millis / Interval::MSECS_PER_SEC);
	}

	static PartAdapter PartCodeAdapterFactory(DatePartSpecifier part) {
		switch (part) {
		case DatePartSpecifier::YEAR:
			return ExtractYear;
		case DatePartSpecifier::MONTH:
			return ExtractMonth;
		case DatePartSpecifier::DAY:
			return ExtractDay;
		case DatePartSpecifier::DECADE:
			return ExtractDecade;
		case DatePartSpecifier::CENTURY:
			return ExtractCentury;
		case DatePartSpecifier::MILLENNIUM:
			return ExtractMillenium;
		case DatePartSpecifier::MICROSECONDS:
			return ExtractMicrosecond;
		case DatePartSpecifier::MILLISECONDS:
			return ExtractMillisecond;
		case DatePartSpecifier::SECOND:
			return ExtractSecond;
		case DatePartSpecifier::MINUTE:
			return ExtractMinute;
		case DatePartSpecifier::HOUR:
			return ExtractHour;
		case DatePartSpecifier::DOW:
			return ExtractDayOfWeek;
		case DatePartSpecifier::ISODOW:
			return ExtractISODayOfWeek;
		case DatePartSpecifier::WEEK:
			return ExtractWeek;
		case DatePartSpecifier::DOY:
			return ExtractDayOfYear;
		case DatePartSpecifier::QUARTER:
			return ExtractQuarter;
		case DatePartSpecifier::YEARWEEK:
			return ExtractYearWeek;
		case DatePartSpecifier::EPOCH:
			return ExtractEpoch;
		default:
			throw Exception("Unsupported ICU extract adapter");
		}
	}

	struct BindData : public FunctionData {
		BindData(CalendarPtr calendar_p, PartAdapter adapter_p) : calendar(move(calendar_p)), adapter(adapter_p) {
		}

		CalendarPtr calendar;
		PartAdapter adapter;

		unique_ptr<FunctionData> Copy() override {
			return make_unique<BindData>(CalendarPtr(calendar->clone()), adapter);
		}
	};

	static void UnaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 1);
		auto &date_arg = args.data[0];

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		UnaryExecutor::Execute<timestamp_t, int32_t>(date_arg, result, args.size(), [&](timestamp_t input) {
			UErrorCode status = U_ZERO_ERROR;

			const UDate millis = input.value / Interval::MICROS_PER_MSEC;
			const auto micros = input.value % Interval::MICROS_PER_MSEC;
			calendar->setTime(millis, status);
			if (U_FAILURE(status)) {
				throw Exception("Unable to compute ICU date part.");
			}
			return info.adapter(calendar.get(), micros);
		});
	}

	static void BinaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);
		auto &part_arg = args.data[0];
		auto &date_arg = args.data[1];

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		BinaryExecutor::Execute<string_t, timestamp_t, int32_t>(
		    part_arg, date_arg, result, args.size(), [&](string_t specifier, timestamp_t input) {
			    UErrorCode status = U_ZERO_ERROR;

			    const UDate millis = input.value / Interval::MICROS_PER_MSEC;
			    const auto micros = input.value % Interval::MICROS_PER_MSEC;
			    calendar->setTime(millis, status);
			    if (U_FAILURE(status)) {
				    throw Exception("Unable to compute ICU date part.");
			    }
			    auto adapter = PartCodeAdapterFactory(GetDatePartSpecifier(specifier.GetString()));
			    return adapter(calendar.get(), micros);
		    });
	}

	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments) {
		Value tz_value;
		string tz_id;
		if (context.TryGetCurrentSetting("TimeZone", tz_value)) {
			tz_id = tz_value.ToString();
		}
		auto tz = icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(tz_id)));

		UErrorCode success = U_ZERO_ERROR;
		CalendarPtr calendar(icu::Calendar::createInstance(tz, success));
		if (U_FAILURE(success)) {
			throw Exception("Unable to create ICU date part calendar.");
		}

		auto adapter =
		    (arguments.size() == 1) ? PartCodeAdapterFactory(PartCodeFromFunction(bound_function.name)) : nullptr;

		return make_unique<BindData>(move(calendar), adapter);
	}

	static ScalarFunction GetUnaryTimestampFunction(const string &name) {
		return ScalarFunction(name, {LogicalType::TIMESTAMP}, LogicalType::INTEGER, UnaryFunction, false, Bind);
	}

	static void AddUnaryTimestampFunction(const string &name, ClientContext &context) {
		auto &catalog = Catalog::GetCatalog(context);
		ScalarFunction func = GetUnaryTimestampFunction(name);
		CreateScalarFunctionInfo func_info(move(func));
		catalog.CreateFunction(context, &func_info);
	}

	static ScalarFunction GetBinaryTimestampFunction(const string &name) {
		return ScalarFunction(name, {LogicalType::VARCHAR, LogicalType::TIMESTAMP}, LogicalType::INTEGER,
		                      BinaryFunction, false, Bind);
	}

	static void AddBinaryTimestampFunction(const string &name, ClientContext &context) {
		auto &catalog = Catalog::GetCatalog(context);
		ScalarFunction func = GetBinaryTimestampFunction(name);
		CreateScalarFunctionInfo func_info(move(func));
		catalog.CreateFunction(context, &func_info);
	}
};

void RegisterICUDatePartFunctions(ClientContext &context) {
	ICUDatePart::AddUnaryTimestampFunction("icu_year", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_month", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_day", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_decade", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_century", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_millennium", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_microsecond", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_millisecond", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_second", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_minute", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_hour", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_dayofweek", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_isodow", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_week", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_dayofyear", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_quarter", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_yearweek", context);
	ICUDatePart::AddUnaryTimestampFunction("icu_epoch", context);

	ICUDatePart::AddBinaryTimestampFunction("icu_date_part", context);
}

} // namespace duckdb
