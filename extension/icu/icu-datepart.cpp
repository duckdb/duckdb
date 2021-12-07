#include "include/icu-datepart.hpp"
#include "include/icu-collate.hpp"

#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/vector_operations/unary_executor.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ICUDatePart {
	using CalendarPtr = unique_ptr<icu::Calendar>;
	typedef int64_t (*part_adapter_t)(icu::Calendar *calendar, const uint64_t micros);

	static DatePartSpecifier PartCodeFromFunction(const string &name) {
		//	Missing part aliases
		if (name == "dayofmonth") {
			return DatePartSpecifier::DAY;
		} else if (name == "weekday") {
			return DatePartSpecifier::DOW;
		} else if (name == "weekofyear") {
			return DatePartSpecifier::WEEK;
		} else {
			return GetDatePartSpecifier(name);
		}
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
	static int64_t ExtractYear(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_YEAR);
	}

	static int64_t ExtractDecade(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractYear(calendar, micros) / 10;
	}

	static int64_t ExtractCentury(icu::Calendar *calendar, const uint64_t micros) {
		return 1 + ExtractYear(calendar, micros) / 100;
	}

	static int64_t ExtractMillenium(icu::Calendar *calendar, const uint64_t micros) {
		return 1 + ExtractYear(calendar, micros) / 1000;
	}

	static int64_t ExtractMonth(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_MONTH) + 1;
	}

	static int64_t ExtractQuarter(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_MONTH) / Interval::MONTHS_PER_QUARTER + 1;
	}

	static int64_t ExtractDay(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_DATE);
	}

	static int64_t ExtractDayOfWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_SUNDAY);
		return ExtractField(calendar, UCAL_DAY_OF_WEEK) - UCAL_SUNDAY;
	}

	static int64_t ExtractISODayOfWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_MONDAY);
		return ExtractField(calendar, UCAL_DAY_OF_WEEK);
	}

	static int64_t ExtractWeek(icu::Calendar *calendar, const uint64_t micros) {
		calendar->setFirstDayOfWeek(UCAL_SUNDAY);
		calendar->setMinimalDaysInFirstWeek(4);
		return ExtractField(calendar, UCAL_WEEK_OF_YEAR);
	}

	static int64_t ExtractYearWeek(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractYear(calendar, micros) * 100 + ExtractWeek(calendar, micros);
	}

	static int64_t ExtractDayOfYear(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_DAY_OF_YEAR);
	}

	static int64_t ExtractHour(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_HOUR_OF_DAY);
	}

	static int64_t ExtractMinute(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_MINUTE);
	}

	static int64_t ExtractSecond(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractField(calendar, UCAL_SECOND);
	}

	static int64_t ExtractMillisecond(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractSecond(calendar, micros) * Interval::MSECS_PER_SEC + ExtractField(calendar, UCAL_MILLISECOND);
	}

	static int64_t ExtractMicrosecond(icu::Calendar *calendar, const uint64_t micros) {
		return ExtractMillisecond(calendar, micros) * Interval::MICROS_PER_MSEC + micros;
	}

	static int64_t ExtractEpoch(icu::Calendar *calendar, const uint64_t micros) {
		UErrorCode status = U_ZERO_ERROR;
		auto millis = calendar->getTime(status);
		millis += ExtractField(calendar, UCAL_ZONE_OFFSET);
		millis += ExtractField(calendar, UCAL_DST_OFFSET);
		//	Truncate
		return millis / Interval::MSECS_PER_SEC;
	}

	static part_adapter_t PartCodeAdapterFactory(DatePartSpecifier part) {
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

	static date_t MakeLastDay(icu::Calendar *calendar, const uint64_t micros) {
		// Set the calendar to midnight on the last day of the month
		calendar->set(UCAL_MILLISECOND, 0);
		calendar->set(UCAL_SECOND, 0);
		calendar->set(UCAL_MINUTE, 0);
		calendar->set(UCAL_HOUR_OF_DAY, 0);

		UErrorCode status = U_ZERO_ERROR;
		const auto dd = calendar->getActualMaximum(UCAL_DATE, status);
		if (U_FAILURE(status)) {
			throw Exception("Unable to extract ICU last day.");
		}

		calendar->set(UCAL_DATE, dd);

		// DATETZ only makes sense as a number of days from the local epoch.
		return Date::EpochToDate(ExtractEpoch(calendar, 0));
	}

	template <typename RESULT_TYPE>
	struct BindData : public FunctionData {
		using result_t = RESULT_TYPE;
		typedef result_t (*adapter_t)(icu::Calendar *calendar, const uint64_t micros);
		BindData(CalendarPtr calendar_p, adapter_t adapter_p) : calendar(move(calendar_p)), adapter(adapter_p) {
		}

		CalendarPtr calendar;
		adapter_t adapter;

		unique_ptr<FunctionData> Copy() override {
			return make_unique<BindData>(CalendarPtr(calendar->clone()), adapter);
		}
	};

	template <typename RESULT_TYPE>
	static void UnaryTimestampFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		using BIND_TYPE = BindData<RESULT_TYPE>;
		D_ASSERT(args.ColumnCount() == 1);
		auto &date_arg = args.data[0];

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BIND_TYPE &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		UnaryExecutor::Execute<timestamp_t, RESULT_TYPE>(date_arg, result, args.size(), [&](timestamp_t input) {
			UErrorCode status = U_ZERO_ERROR;

			const int64_t millis = input.value / Interval::MICROS_PER_MSEC;
			const uint64_t micros = input.value % Interval::MICROS_PER_MSEC;
			const auto udate = UDate(millis);
			calendar->setTime(udate, status);
			if (U_FAILURE(status)) {
				throw Exception("Unable to compute ICU date part.");
			}
			return info.adapter(calendar.get(), micros);
		});
	}

	template <typename RESULT_TYPE>
	static void BinaryTimestampFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		using BIND_TYPE = BindData<int64_t>;
		D_ASSERT(args.ColumnCount() == 2);
		auto &part_arg = args.data[0];
		auto &date_arg = args.data[1];

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BIND_TYPE &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		BinaryExecutor::Execute<string_t, timestamp_t, RESULT_TYPE>(
		    part_arg, date_arg, result, args.size(), [&](string_t specifier, timestamp_t input) {
			    UErrorCode status = U_ZERO_ERROR;

			    const int64_t millis = input.value / Interval::MICROS_PER_MSEC;
			    const uint64_t micros = input.value % Interval::MICROS_PER_MSEC;
			    const auto udate = UDate(millis);
			    calendar->setTime(udate, status);
			    if (U_FAILURE(status)) {
				    throw Exception("Unable to compute ICU date part.");
			    }
			    auto adapter = PartCodeAdapterFactory(GetDatePartSpecifier(specifier.GetString()));
			    return adapter(calendar.get(), micros);
		    });
	}

	template <typename BIND_TYPE>
	static unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                     vector<unique_ptr<Expression>> &arguments,
	                                     typename BIND_TYPE::adapter_t adapter) {
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

		return make_unique<BIND_TYPE>(move(calendar), adapter);
	}

	static unique_ptr<FunctionData> BindDatePart(ClientContext &context, ScalarFunction &bound_function,
	                                             vector<unique_ptr<Expression>> &arguments) {
		using data_t = BindData<int64_t>;
		auto adapter =
		    (arguments.size() == 1) ? PartCodeAdapterFactory(PartCodeFromFunction(bound_function.name)) : nullptr;
		return Bind<data_t>(context, bound_function, arguments, adapter);
	}

	static ScalarFunction GetUnaryPartCodeFunction(const string &name) {
		return ScalarFunction(name, {LogicalType::TIMESTAMP_TZ}, LogicalType::BIGINT,
		                      UnaryTimestampFunction<int64_t>, false, BindDatePart);
	}

	static void AddUnaryPartCodeFunction(const string &name, ClientContext &context) {
		auto &catalog = Catalog::GetCatalog(context);
		ScalarFunction func = GetUnaryPartCodeFunction(name);
		CreateScalarFunctionInfo func_info(move(func));
		catalog.AddFunction(context, &func_info);
	}

	static ScalarFunction GetBinaryPartCodeFunction(const string &name) {
		return ScalarFunction(name, {LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ}, LogicalType::BIGINT,
		                      BinaryTimestampFunction<int64_t>, false, BindDatePart);
	}

	static void AddBinaryPartCodeFunction(const string &name, ClientContext &context) {
		auto &catalog = Catalog::GetCatalog(context);
		ScalarFunction func = GetBinaryPartCodeFunction(name);
		CreateScalarFunctionInfo func_info(move(func));
		catalog.AddFunction(context, &func_info);
	}

	static unique_ptr<FunctionData> BindLastDate(ClientContext &context, ScalarFunction &bound_function,
	                                             vector<unique_ptr<Expression>> &arguments) {
		using data_t = BindData<date_t>;
		return Bind<data_t>(context, bound_function, arguments, MakeLastDay);
	}

	static ScalarFunction GetLastDayFunction(const string &name) {
		return ScalarFunction(name, {LogicalType::TIMESTAMP_TZ}, LogicalType::DATE_TZ,
		                      UnaryTimestampFunction<date_t>, false, BindLastDate);
	}
	static void AddLastDayFunction(const string &name, ClientContext &context) {
		auto &catalog = Catalog::GetCatalog(context);
		ScalarFunction func = GetLastDayFunction(name);
		CreateScalarFunctionInfo func_info(move(func));
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUDatePartFunctions(ClientContext &context) {
	// register the individual operators
	ICUDatePart::AddUnaryPartCodeFunction("year", context);
	ICUDatePart::AddUnaryPartCodeFunction("month", context);
	ICUDatePart::AddUnaryPartCodeFunction("day", context);
	ICUDatePart::AddUnaryPartCodeFunction("decade", context);
	ICUDatePart::AddUnaryPartCodeFunction("century", context);
	ICUDatePart::AddUnaryPartCodeFunction("millennium", context);
	ICUDatePart::AddUnaryPartCodeFunction("microsecond", context);
	ICUDatePart::AddUnaryPartCodeFunction("millisecond", context);
	ICUDatePart::AddUnaryPartCodeFunction("second", context);
	ICUDatePart::AddUnaryPartCodeFunction("minute", context);
	ICUDatePart::AddUnaryPartCodeFunction("hour", context);
	ICUDatePart::AddUnaryPartCodeFunction("dayofweek", context);
	ICUDatePart::AddUnaryPartCodeFunction("isodow", context);
	ICUDatePart::AddUnaryPartCodeFunction("week", context); //  Note that WeekOperator is ISO-8601, not US
	ICUDatePart::AddUnaryPartCodeFunction("dayofyear", context);
	ICUDatePart::AddUnaryPartCodeFunction("quarter", context);
	ICUDatePart::AddUnaryPartCodeFunction("epoch", context);

	//  register combinations
	ICUDatePart::AddUnaryPartCodeFunction("yearweek", context);

	//  register various aliases
	ICUDatePart::AddUnaryPartCodeFunction("dayofmonth", context);
	ICUDatePart::AddUnaryPartCodeFunction("weekday", context);
	ICUDatePart::AddUnaryPartCodeFunction("weekofyear", context);

	//  register the last_day function
	ICUDatePart::AddLastDayFunction("last_day", context);

	// finally the actual date_part function
	ICUDatePart::AddBinaryPartCodeFunction("date_part", context);
	ICUDatePart::AddBinaryPartCodeFunction("datepart", context);
}

} // namespace duckdb
