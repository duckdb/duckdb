#include "include/icu-datetrunc.hpp"
#include "include/icu-collate.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ICUDateTrunc {
	using CalendarPtr = unique_ptr<icu::Calendar>;
	typedef void (*part_truncator_t)(icu::Calendar *calendar, uint64_t &micros);

	struct BindData : public FunctionData {
		explicit BindData(CalendarPtr calendar_p) : calendar(move(calendar_p)) {
		}

		CalendarPtr calendar;

		unique_ptr<FunctionData> Copy() override {
			return make_unique<BindData>(CalendarPtr(calendar->clone()));
		}
	};

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
			throw Exception("Unable to create ICU DATETRUNC calendar.");
		}

		return make_unique<BindData>(move(calendar));
	}

	static int32_t ExtractField(icu::Calendar *calendar, UCalendarDateFields field) {
		UErrorCode status = U_ZERO_ERROR;
		const auto result = calendar->get(field, status);
		if (U_FAILURE(status)) {
			throw Exception("Unable to extract ICU date part.");
		}
		return result;
	}

	static void TruncMicrosecond(icu::Calendar *calendar, uint64_t &micros) {
	}

	static void TruncMillisecond(icu::Calendar *calendar, uint64_t &micros) {
		TruncMicrosecond(calendar, micros);
		micros = 0;
	}

	static void TruncSecond(icu::Calendar *calendar, uint64_t &micros) {
		TruncMillisecond(calendar, micros);
		calendar->set(UCAL_MILLISECOND, 0);
	}

	static void TruncMinute(icu::Calendar *calendar, uint64_t &micros) {
		TruncSecond(calendar, micros);
		calendar->set(UCAL_SECOND, 0);
	}

	static void TruncHour(icu::Calendar *calendar, uint64_t &micros) {
		TruncMinute(calendar, micros);
		calendar->set(UCAL_MINUTE, 0);
	}

	static void TruncDay(icu::Calendar *calendar, uint64_t &micros) {
		TruncHour(calendar, micros);
		calendar->set(UCAL_HOUR_OF_DAY, 0);
	}

	static void TruncWeek(icu::Calendar *calendar, uint64_t &micros) {
		calendar->setFirstDayOfWeek(UCAL_MONDAY);
		TruncDay(calendar, micros);
		calendar->set(UCAL_DAY_OF_WEEK, UCAL_MONDAY);
	}

	static void TruncMonth(icu::Calendar *calendar, uint64_t &micros) {
		TruncDay(calendar, micros);
		calendar->set(UCAL_DATE, 1);
	}

	static void TruncQuarter(icu::Calendar *calendar, uint64_t &micros) {
		TruncMonth(calendar, micros);
		auto mm = ExtractField(calendar, UCAL_MONTH);
		calendar->set(UCAL_MONTH, (mm / 3) * 3);
	}

	static void TruncYear(icu::Calendar *calendar, uint64_t &micros) {
		TruncMonth(calendar, micros);
		calendar->set(UCAL_MONTH, UCAL_JANUARY);
	}

	static void TruncDecade(icu::Calendar *calendar, uint64_t &micros) {
		TruncYear(calendar, micros);
		auto yyyy = ExtractField(calendar, UCAL_YEAR) / 10;
		calendar->set(UCAL_YEAR, yyyy * 10);
	}

	static void TruncCentury(icu::Calendar *calendar, uint64_t &micros) {
		TruncYear(calendar, micros);
		auto yyyy = ExtractField(calendar, UCAL_YEAR) / 100;
		calendar->set(UCAL_YEAR, yyyy * 100);
	}

	static void TruncMillenium(icu::Calendar *calendar, uint64_t &micros) {
		TruncYear(calendar, micros);
		auto yyyy = ExtractField(calendar, UCAL_YEAR) / 1000;
		calendar->set(UCAL_YEAR, yyyy * 1000);
	}

	static part_truncator_t TruncatorFactory(DatePartSpecifier type) {
		switch (type) {
		case DatePartSpecifier::MILLENNIUM:
			return TruncMillenium;
		case DatePartSpecifier::CENTURY:
			return TruncCentury;
		case DatePartSpecifier::DECADE:
			return TruncDecade;
		case DatePartSpecifier::YEAR:
			return TruncYear;
		case DatePartSpecifier::QUARTER:
			return TruncQuarter;
		case DatePartSpecifier::MONTH:
			return TruncMonth;
		case DatePartSpecifier::WEEK:
		case DatePartSpecifier::YEARWEEK:
			return TruncWeek;
		case DatePartSpecifier::DAY:
		case DatePartSpecifier::DOW:
		case DatePartSpecifier::ISODOW:
		case DatePartSpecifier::DOY:
			return TruncDay;
		case DatePartSpecifier::HOUR:
			return TruncHour;
		case DatePartSpecifier::MINUTE:
			return TruncMinute;
		case DatePartSpecifier::SECOND:
		case DatePartSpecifier::EPOCH:
			return TruncSecond;
		case DatePartSpecifier::MILLISECONDS:
			return TruncMillisecond;
		case DatePartSpecifier::MICROSECONDS:
			return TruncMicrosecond;
		default:
			throw NotImplementedException("Specifier type not implemented for DATETRUNC");
		}
	}
	static void BinaryFunction(DataChunk &args, ExpressionState &state, Vector &result) {
		D_ASSERT(args.ColumnCount() == 2);
		auto &part_arg = args.data[0];
		auto &date_arg = args.data[1];

		auto &func_expr = (BoundFunctionExpression &)state.expr;
		auto &info = (BindData &)*func_expr.bind_info;
		CalendarPtr calendar(info.calendar->clone());

		BinaryExecutor::Execute<string_t, timestamp_t, timestamp_t>(
		    part_arg, date_arg, result, args.size(), [&](string_t specifier, timestamp_t input) {
			    UErrorCode status = U_ZERO_ERROR;

			    int64_t millis = input.value / Interval::MICROS_PER_MSEC;
			    uint64_t micros = input.value % Interval::MICROS_PER_MSEC;
			    const auto udate = UDate(millis);
			    calendar->setTime(udate, status);
			    if (U_FAILURE(status)) {
				    throw Exception("Unable to compute ICU DATETRUNC.");
			    }
			    auto truncator = TruncatorFactory(GetDatePartSpecifier(specifier.GetString()));
			    truncator(calendar.get(), micros);

			    millis = int64_t(calendar->getTime(status));
			    if (U_FAILURE(status)) {
				    throw Exception("Unable to compute ICU DATETRUNC.");
			    }
			    millis *= Interval::MICROS_PER_MSEC;
			    millis += micros;
			    return timestamp_t(millis);
		    });
	}

	static ScalarFunction GetBinaryTimestampFunction(const string &name) {
		return ScalarFunction(name, {LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ}, LogicalType::TIMESTAMP_TZ,
		                      BinaryFunction, false, Bind);
	}

	static void AddBinaryTimestampFunction(const string &name, ClientContext &context) {
		auto add_func = GetBinaryTimestampFunction(name);

		ScalarFunctionSet set(add_func.name);

		// Extract any previous functions into the set
		auto &catalog = Catalog::GetCatalog(context);
		auto funcs = catalog.GetEntry(context, CatalogType::SCALAR_FUNCTION_ENTRY, DEFAULT_SCHEMA, add_func.name, true);
		if (funcs) {
			auto &entry = *(ScalarFunctionCatalogEntry *)funcs;
			for (const auto &f : entry.functions) {
				set.AddFunction(f);
			}
		}

		// Add the new one
		set.AddFunction(add_func);

		CreateScalarFunctionInfo func_info(move(set));
		func_info.on_conflict = OnCreateConflict::REPLACE_ON_CONFLICT;

		catalog.CreateFunction(context, &func_info);
	}
};

void RegisterICUDateTruncFunctions(ClientContext &context) {
	ICUDateTrunc::AddBinaryTimestampFunction("date_trunc", context);
	ICUDateTrunc::AddBinaryTimestampFunction("datetrunc", context);
}

} // namespace duckdb
