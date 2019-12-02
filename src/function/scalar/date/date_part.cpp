#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"
using namespace std;

namespace duckdb {

enum class SpecifierType {
	YEAR,
	MONTH,
	DAY,
	DECADE,
	CENTURY,
	MILLENIUM,
	MICROSECONDS,
	MILLISECONDS,
	SECOND,
	MINUTE,
	HOUR,
	EPOCH,
	DOW,
	ISODOW,
	WEEK,
	QUARTER,
	DOY
};

static SpecifierType GetSpecifierType(string specifier) {
	specifier = StringUtil::Lower(specifier);
	if (specifier == "year") {
		return SpecifierType::YEAR;
	} else if (specifier == "month") {
		return SpecifierType::MONTH;
	} else if (specifier == "day") {
		return SpecifierType::DAY;
	} else if (specifier == "decade") {
		return SpecifierType::DECADE;
	} else if (specifier == "century") {
		return SpecifierType::CENTURY;
	} else if (specifier == "millennium") {
		return SpecifierType::MILLENIUM;
	} else if (specifier == "microseconds") {
		return SpecifierType::MICROSECONDS;
	} else if (specifier == "milliseconds") {
		return SpecifierType::MILLISECONDS;
	} else if (specifier == "second") {
		return SpecifierType::SECOND;
	} else if (specifier == "minute") {
		return SpecifierType::MINUTE;
	} else if (specifier == "hour") {
		return SpecifierType::HOUR;
	} else if (specifier == "epoch") {
		// seconds since 1970-01-01
		return SpecifierType::EPOCH;
	} else if (specifier == "dow") {
		// day of the week (Sunday = 0, Saturday = 6)
		return SpecifierType::DOW;
	} else if (specifier == "isodow") {
		// isodow (Monday = 1, Sunday = 7)
		return SpecifierType::ISODOW;
	} else if (specifier == "week") {
		// week number
		return SpecifierType::WEEK;
	} else if (specifier == "doy") {
		// day of the year (1-365/366)
		return SpecifierType::DOY;
	} else if (specifier == "quarter") {
		// quarter of the year (1-4)
		return SpecifierType::QUARTER;
	} else {
		throw ConversionException("extract specifier \"%s\" not recognized", specifier.c_str());
	}
}

static int64_t extract_element(SpecifierType type, date_t element) {
	switch (type) {
	case SpecifierType::YEAR:
		return Date::ExtractYear(element);
	case SpecifierType::MONTH:
		return Date::ExtractMonth(element);
	case SpecifierType::DAY:
		return Date::ExtractDay(element);
	case SpecifierType::DECADE:
		return Date::ExtractYear(element) / 10;
	case SpecifierType::CENTURY:
		return ((Date::ExtractYear(element) - 1) / 100) + 1;
	case SpecifierType::MILLENIUM:
		return ((Date::ExtractYear(element) - 1) / 1000) + 1;
	case SpecifierType::QUARTER:
		return Date::ExtractMonth(element) / 4;
	case SpecifierType::EPOCH:
		return Date::Epoch(element);
	case SpecifierType::DOW:
		// day of the week (Sunday = 0, Saturday = 6)
		// turn sunday into 0 by doing mod 7
		return Date::ExtractISODayOfTheWeek(element) % 7;
	case SpecifierType::ISODOW:
		// isodow (Monday = 1, Sunday = 7)
		return Date::ExtractISODayOfTheWeek(element);
	case SpecifierType::DOY:
		return Date::ExtractDayOfTheYear(element);
	case SpecifierType::WEEK:
		return Date::ExtractWeekNumber(element);
	case SpecifierType::MICROSECONDS:
	case SpecifierType::MILLISECONDS:
	case SpecifierType::SECOND:
	case SpecifierType::MINUTE:
	case SpecifierType::HOUR:
		return 0;
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

static int64_t extract_element(SpecifierType type, timestamp_t element) {
	switch (type) {
	case SpecifierType::YEAR:
	case SpecifierType::MONTH:
	case SpecifierType::DAY:
	case SpecifierType::DECADE:
	case SpecifierType::CENTURY:
	case SpecifierType::MILLENIUM:
	case SpecifierType::QUARTER:
	case SpecifierType::DOW:
	case SpecifierType::ISODOW:
	case SpecifierType::DOY:
	case SpecifierType::WEEK:
		return extract_element(type, Timestamp::GetDate(element));
	case SpecifierType::EPOCH:
		return Timestamp::GetEpoch(element);
	case SpecifierType::MILLISECONDS:
		return Timestamp::GetMilliseconds(element);
	case SpecifierType::SECOND:
		return Timestamp::GetSeconds(element);
	case SpecifierType::MINUTE:
		return Timestamp::GetMinutes(element);
	case SpecifierType::HOUR:
		return Timestamp::GetHours(element);
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

static void date_part_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                               BoundFunctionExpression &expr, Vector &result) {
	result.Initialize(TypeId::BIGINT);
	result.nullmask = inputs[1].nullmask;
	result.count = inputs[1].count;
	result.sel_vector = inputs[1].sel_vector;
	if (inputs[1].type != TypeId::INTEGER) {
		throw NotImplementedException("Can only extract from dates or timestamps");
	}

	auto result_data = (int64_t *)result.data;
	if (inputs[0].IsConstant()) {
		// constant specifier
		auto specifier_type = GetSpecifierType(((const char **)inputs[0].data)[0]);
		VectorOperations::ExecType<date_t>(inputs[1], [&](date_t element, index_t i, index_t k) {
			result_data[i] = extract_element(specifier_type, element);
		});
	} else {
		// not constant specifier
		auto specifiers = ((const char **)inputs[0].data);
		VectorOperations::ExecType<date_t>(inputs[1], [&](date_t element, index_t i, index_t k) {
			result_data[i] = extract_element(GetSpecifierType(specifiers[i]), element);
		});
	}
}

static void timestamp_part_function(ExpressionExecutor &exec, Vector inputs[], index_t input_count,
                                    BoundFunctionExpression &expr, Vector &result) {
	result.Initialize(TypeId::BIGINT);
	result.nullmask = inputs[1].nullmask;
	result.count = inputs[1].count;
	result.sel_vector = inputs[1].sel_vector;
	if (inputs[1].type != TypeId::BIGINT) {
		throw NotImplementedException("Can only extract from dates or timestamps");
	}

	auto result_data = (int64_t *)result.data;
	if (inputs[0].IsConstant()) {
		// constant specifier
		auto specifier_type = GetSpecifierType(((const char **)inputs[0].data)[0]);
		VectorOperations::ExecType<timestamp_t>(inputs[1], [&](timestamp_t element, index_t i, index_t k) {
			result_data[i] = extract_element(specifier_type, element);
		});
	} else {
		// not constant specifier
		auto specifiers = ((const char **)inputs[0].data);
		VectorOperations::ExecType<timestamp_t>(inputs[1], [&](timestamp_t element, index_t i, index_t k) {
			result_data[i] = extract_element(GetSpecifierType(specifiers[i]), element);
		});
	}
}

void DatePartFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet date_part("date_part");
	date_part.AddFunction(ScalarFunction({SQLType::VARCHAR, SQLType::DATE}, SQLType::BIGINT, date_part_function));
	date_part.AddFunction(
	    ScalarFunction({SQLType::VARCHAR, SQLType::TIMESTAMP}, SQLType::BIGINT, timestamp_part_function));
	set.AddFunction(date_part);
	date_part.name = "datepart";
	set.AddFunction(date_part);
}

} // namespace duckdb
