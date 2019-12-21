#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/vector_operations/vector_operations.hpp"
#include "duckdb/common/string_util.hpp"
using namespace std;

namespace duckdb {

DatePartSpecifier GetDatePartSpecifier(string specifier) {
	specifier = StringUtil::Lower(specifier);
	if (specifier == "year") {
		return DatePartSpecifier::YEAR;
	} else if (specifier == "month") {
		return DatePartSpecifier::MONTH;
	} else if (specifier == "day") {
		return DatePartSpecifier::DAY;
	} else if (specifier == "decade") {
		return DatePartSpecifier::DECADE;
	} else if (specifier == "century") {
		return DatePartSpecifier::CENTURY;
	} else if (specifier == "millennium") {
		return DatePartSpecifier::MILLENIUM;
	} else if (specifier == "microseconds") {
		return DatePartSpecifier::MICROSECONDS;
	} else if (specifier == "milliseconds") {
		return DatePartSpecifier::MILLISECONDS;
	} else if (specifier == "second") {
		return DatePartSpecifier::SECOND;
	} else if (specifier == "minute") {
		return DatePartSpecifier::MINUTE;
	} else if (specifier == "hour") {
		return DatePartSpecifier::HOUR;
	} else if (specifier == "epoch") {
		// seconds since 1970-01-01
		return DatePartSpecifier::EPOCH;
	} else if (specifier == "dow") {
		// day of the week (Sunday = 0, Saturday = 6)
		return DatePartSpecifier::DOW;
	} else if (specifier == "isodow") {
		// isodow (Monday = 1, Sunday = 7)
		return DatePartSpecifier::ISODOW;
	} else if (specifier == "week") {
		// week number
		return DatePartSpecifier::WEEK;
	} else if (specifier == "doy") {
		// day of the year (1-365/366)
		return DatePartSpecifier::DOY;
	} else if (specifier == "quarter") {
		// quarter of the year (1-4)
		return DatePartSpecifier::QUARTER;
	} else {
		throw ConversionException("extract specifier \"%s\" not recognized", specifier.c_str());
	}
}

static int64_t extract_element(DatePartSpecifier type, date_t element) {
	switch (type) {
	case DatePartSpecifier::YEAR:
		return Date::ExtractYear(element);
	case DatePartSpecifier::MONTH:
		return Date::ExtractMonth(element);
	case DatePartSpecifier::DAY:
		return Date::ExtractDay(element);
	case DatePartSpecifier::DECADE:
		return Date::ExtractYear(element) / 10;
	case DatePartSpecifier::CENTURY:
		return ((Date::ExtractYear(element) - 1) / 100) + 1;
	case DatePartSpecifier::MILLENIUM:
		return ((Date::ExtractYear(element) - 1) / 1000) + 1;
	case DatePartSpecifier::QUARTER:
		return Date::ExtractMonth(element) / 4;
	case DatePartSpecifier::EPOCH:
		return Date::Epoch(element);
	case DatePartSpecifier::DOW:
		// day of the week (Sunday = 0, Saturday = 6)
		// turn sunday into 0 by doing mod 7
		return Date::ExtractISODayOfTheWeek(element) % 7;
	case DatePartSpecifier::ISODOW:
		// isodow (Monday = 1, Sunday = 7)
		return Date::ExtractISODayOfTheWeek(element);
	case DatePartSpecifier::DOY:
		return Date::ExtractDayOfTheYear(element);
	case DatePartSpecifier::WEEK:
		return Date::ExtractWeekNumber(element);
	case DatePartSpecifier::MICROSECONDS:
	case DatePartSpecifier::MILLISECONDS:
	case DatePartSpecifier::SECOND:
	case DatePartSpecifier::MINUTE:
	case DatePartSpecifier::HOUR:
		return 0;
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

static int64_t extract_element(DatePartSpecifier type, timestamp_t element) {
	switch (type) {
	case DatePartSpecifier::YEAR:
	case DatePartSpecifier::MONTH:
	case DatePartSpecifier::DAY:
	case DatePartSpecifier::DECADE:
	case DatePartSpecifier::CENTURY:
	case DatePartSpecifier::MILLENIUM:
	case DatePartSpecifier::QUARTER:
	case DatePartSpecifier::DOW:
	case DatePartSpecifier::ISODOW:
	case DatePartSpecifier::DOY:
	case DatePartSpecifier::WEEK:
		return extract_element(type, Timestamp::GetDate(element));
	case DatePartSpecifier::EPOCH:
		return Timestamp::GetEpoch(element);
	case DatePartSpecifier::MILLISECONDS:
		return Timestamp::GetMilliseconds(element);
	case DatePartSpecifier::SECOND:
		return Timestamp::GetSeconds(element);
	case DatePartSpecifier::MINUTE:
		return Timestamp::GetMinutes(element);
	case DatePartSpecifier::HOUR:
		return Timestamp::GetHours(element);
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

struct DatePartOperator {
	template <class T> static inline int64_t Operation(const char* specifier, T date) {
		return extract_element(GetDatePartSpecifier(specifier), date);
	}
};

void DatePartFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet date_part("date_part");
	date_part.AddFunction(ScalarFunction({SQLType::VARCHAR, SQLType::DATE}, SQLType::BIGINT, ScalarFunction::BinaryFunction<const char*, date_t, int64_t, DatePartOperator>));
	date_part.AddFunction(
	    ScalarFunction({SQLType::VARCHAR, SQLType::TIMESTAMP}, SQLType::BIGINT, ScalarFunction::BinaryFunction<const char*, timestamp_t, int64_t, DatePartOperator>));
	set.AddFunction(date_part);
	date_part.name = "datepart";
	set.AddFunction(date_part);
}

} // namespace duckdb
