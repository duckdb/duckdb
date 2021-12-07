#include "include/icu-datetrunc.hpp"
#include "include/icu-datefunc.hpp"

#include "duckdb/catalog/catalog_entry/scalar_function_catalog_entry.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/vector_operations/binary_executor.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/parser/parsed_data/create_scalar_function_info.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"

namespace duckdb {

struct ICUCalendarTrunc {
	typedef void (*part_truncator_t)(icu::Calendar *calendar, uint64_t &micros);

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

	template <class TA, class TB, class TR>
	static inline TR Operation(TA left, TB right, icu::Calendar *calendar) {
		throw InternalException("Unimplemented type for ICUDateTrunc");
	}
};

template <>
timestamp_t ICUCalendarTrunc::Operation(string_t specifier, timestamp_t input, icu::Calendar *calendar) {
	UErrorCode status = U_ZERO_ERROR;

	int64_t millis = input.value / Interval::MICROS_PER_MSEC;
	uint64_t micros = input.value % Interval::MICROS_PER_MSEC;
	const auto udate = UDate(millis);
	calendar->setTime(udate, status);
	if (U_FAILURE(status)) {
		throw Exception("Unable to compute ICUDateTrunc.");
	}
	auto truncator = TruncatorFactory(GetDatePartSpecifier(specifier.GetString()));
	truncator(calendar, micros);

	millis = int64_t(calendar->getTime(status));
	if (U_FAILURE(status)) {
		throw Exception("Unable to compute ICUDateTrunc.");
	}
	millis *= Interval::MICROS_PER_MSEC;
	millis += micros;
	return timestamp_t(millis);
}

struct ICUDateTrunc : public ICUDateFunc {
	static ScalarFunction GetDateTruncFunction() {
		return GetBinaryDateFunction<string_t, timestamp_t, timestamp_t, ICUCalendarTrunc>(
		    LogicalType::VARCHAR, LogicalType::TIMESTAMP_TZ, LogicalType::TIMESTAMP_TZ);
	}

	static void AddBinaryTimestampFunction(const string &name, ClientContext &context) {
		ScalarFunctionSet set(name);
		set.AddFunction(GetDateTruncFunction());

		CreateScalarFunctionInfo func_info(set);
		auto &catalog = Catalog::GetCatalog(context);
		catalog.AddFunction(context, &func_info);
	}
};

void RegisterICUDateTruncFunctions(ClientContext &context) {
	ICUDateTrunc::AddBinaryTimestampFunction("date_trunc", context);
	ICUDateTrunc::AddBinaryTimestampFunction("datetrunc", context);
}

} // namespace duckdb
