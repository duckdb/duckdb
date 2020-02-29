#include "duckdb/function/scalar/date_functions.hpp"
#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/string_util.hpp"

// TODO date_trunc function should also handle interval data type when it is implemented. See
// https://www.postgresql.org/docs/9.1/functions-datetime.html

using namespace std;

namespace duckdb {

struct MillenniumTruncOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		date_t date = Timestamp::GetDate(input);
		return Timestamp::FromDatetime(Date::FromDate((Date::ExtractYear(date) / 1000) * 1000, 1, 1), 0);
	}
};
template <> timestamp_t MillenniumTruncOperator::Operation(date_t input) {
	return MillenniumTruncOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, 0));
}

struct CenturyTruncOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		date_t date = Timestamp::GetDate(input);
		return Timestamp::FromDatetime(Date::FromDate((Date::ExtractYear(date) / 100) * 100, 1, 1), 0);
	}
};
template <> timestamp_t CenturyTruncOperator::Operation(date_t input) {
	return CenturyTruncOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, 0));
}

struct DecadeTruncOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		date_t date = Timestamp::GetDate(input);
		return Timestamp::FromDatetime(Date::FromDate((Date::ExtractYear(date) / 10) * 10, 1, 1), 0);
	}
};
template <> timestamp_t DecadeTruncOperator::Operation(date_t input) {
	return DecadeTruncOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, 0));
}

struct YearTruncOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		date_t date = Timestamp::GetDate(input);
		return Timestamp::FromDatetime(Date::FromDate(Date::ExtractYear(date), 1, 1), 0);
	}
};
template <> timestamp_t YearTruncOperator::Operation(date_t input) {
	return YearTruncOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, 0));
}

struct QuarterTruncOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		date_t date = Timestamp::GetDate(input);

		int32_t month = Date::ExtractMonth(date);
		month = 1 + (((month - 1) / 3) * 3);
		return Timestamp::FromDatetime(Date::FromDate(Date::ExtractYear(date), month, 1), 0);
	}
};
template <> timestamp_t QuarterTruncOperator::Operation(date_t input) {
	return QuarterTruncOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, 0));
}

struct MonthTruncOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		date_t date = Timestamp::GetDate(input);
		return Timestamp::FromDatetime(Date::FromDate(Date::ExtractYear(date), Date::ExtractMonth(date), 1), 0);
	}
};
template <> timestamp_t MonthTruncOperator::Operation(date_t input) {
	return MonthTruncOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, 0));
}

struct WeekTruncOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		date_t date = Timestamp::GetDate(input);

		return Timestamp::FromDatetime(Date::GetMondayOfCurrentWeek(date), 0);
	}
};
template <> timestamp_t WeekTruncOperator::Operation(date_t input) {
	return WeekTruncOperator::Operation<timestamp_t, timestamp_t>(Timestamp::FromDatetime(input, 0));
}

struct DayTruncOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		date_t date = Timestamp::GetDate(input);
		return Timestamp::FromDatetime(date, 0);
	}
};
template <> timestamp_t DayTruncOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(input, 0);
}

struct HourTruncOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		date_t date = Timestamp::GetDate(input);
		return Timestamp::FromDatetime(date, Time::FromTime(Timestamp::GetHours(input), 0, 0, 0));
	}
};
template <> timestamp_t HourTruncOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(input, 0);
}

struct MinuteTruncOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		date_t date = Timestamp::GetDate(input);
		return Timestamp::FromDatetime(date,
		                               Time::FromTime(Timestamp::GetHours(input), Timestamp::GetMinutes(input), 0, 0));
	}
};
template <> timestamp_t MinuteTruncOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(input, 0);
}

struct SecondsTruncOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		date_t date = Timestamp::GetDate(input);
		return Timestamp::FromDatetime(date, Time::FromTime(Timestamp::GetHours(input), Timestamp::GetMinutes(input),
		                                                    Timestamp::GetSeconds(input), 0));
	}
};
template <> timestamp_t SecondsTruncOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(input, 0);
}

struct MilliSecondsTruncOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return input;
	}
};
template <> timestamp_t MilliSecondsTruncOperator::Operation(date_t input) {
	return Timestamp::FromDatetime(input, 0);
}

template <class TA, class TR> static TR truncate_element(DatePartSpecifier type, TA element) {
	switch (type) {
	case DatePartSpecifier::MILLENNIUM:
		return MillenniumTruncOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::CENTURY:
		return CenturyTruncOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::DECADE:
		return DecadeTruncOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::YEAR:
		return YearTruncOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::QUARTER:
		return QuarterTruncOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::MONTH:
		return MonthTruncOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::WEEK:
		return WeekTruncOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::DAY:
		return DayTruncOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::HOUR:
		return HourTruncOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::MINUTE:
		return MinuteTruncOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::SECOND:
		return SecondsTruncOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::MILLISECONDS:
		return MilliSecondsTruncOperator::Operation<TA, TR>(element);
	case DatePartSpecifier::MICROSECONDS:
		// Since microseconds are not stored truncating to microseconds does the same as to milliseconds.
		return MilliSecondsTruncOperator::Operation<TA, TR>(element);
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

struct DateTruncOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA specifier, TB date) {
		return truncate_element<TB, TR>(GetDatePartSpecifier(specifier.GetString()), date);
	}
};

void DateTruncFun::RegisterFunction(BuiltinFunctions &set) {
	ScalarFunctionSet date_trunc("date_trunc");
	date_trunc.AddFunction(
	    ScalarFunction({SQLType::VARCHAR, SQLType::TIMESTAMP}, SQLType::TIMESTAMP,
	                   ScalarFunction::BinaryFunction<string_t, timestamp_t, timestamp_t, DateTruncOperator>));
	date_trunc.AddFunction(
	    ScalarFunction({SQLType::VARCHAR, SQLType::DATE}, SQLType::TIMESTAMP,
	                   ScalarFunction::BinaryFunction<string_t, date_t, timestamp_t, DateTruncOperator>));
	set.AddFunction(date_trunc);
	date_trunc.name = "datetrunc";
	set.AddFunction(date_trunc);
}

} // namespace duckdb
