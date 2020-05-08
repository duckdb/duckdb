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
		return DatePartSpecifier::MILLENNIUM;
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

struct YearOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractYear(input);
	}
};

template <> int64_t YearOperator::Operation(timestamp_t input) {
	return YearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct MonthOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractMonth(input);
	}
};

template <> int64_t MonthOperator::Operation(timestamp_t input) {
	return MonthOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct DayOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractDay(input);
	}
};

template <> int64_t DayOperator::Operation(timestamp_t input) {
	return DayOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct DecadeOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractYear(input) / 10;
	}
};

template <> int64_t DecadeOperator::Operation(timestamp_t input) {
	return DecadeOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct CenturyOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return ((Date::ExtractYear(input) - 1) / 100) + 1;
	}
};

template <> int64_t CenturyOperator::Operation(timestamp_t input) {
	return CenturyOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct MilleniumOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return ((Date::ExtractYear(input) - 1) / 1000) + 1;
	}
};

template <> int64_t MilleniumOperator::Operation(timestamp_t input) {
	return MilleniumOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct QuarterOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractMonth(input) / 4;
	}
};

template <> int64_t QuarterOperator::Operation(timestamp_t input) {
	return QuarterOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct DayOfWeekOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		// day of the week (Sunday = 0, Saturday = 6)
		// turn sunday into 0 by doing mod 7
		return Date::ExtractISODayOfTheWeek(input) % 7;
	}
};

template <> int64_t DayOfWeekOperator::Operation(timestamp_t input) {
	return DayOfWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct ISODayOfWeekOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		// isodow (Monday = 1, Sunday = 7)
		return Date::ExtractISODayOfTheWeek(input);
	}
};

template <> int64_t ISODayOfWeekOperator::Operation(timestamp_t input) {
	return ISODayOfWeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct DayOfYearOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractDayOfTheYear(input);
	}
};

template <> int64_t DayOfYearOperator::Operation(timestamp_t input) {
	return DayOfYearOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct WeekOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::ExtractWeekNumber(input);
	}
};

template <> int64_t WeekOperator::Operation(timestamp_t input) {
	return WeekOperator::Operation<date_t, int64_t>(Timestamp::GetDate(input));
}

struct YearWeekOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return YearOperator::Operation<TA, TR>(input) * 100 + WeekOperator::Operation<TA, TR>(input);
	}
};

struct EpochOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return Date::Epoch(input);
	}
};

template <> int64_t EpochOperator::Operation(timestamp_t input) {
	return Timestamp::GetEpoch(input);
}

struct MicrosecondsOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 0;
	}
};

template <> int64_t MicrosecondsOperator::Operation(timestamp_t input) {
	return Timestamp::GetMilliseconds(input) * 1000;
}

struct MillisecondsOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 0;
	}
};

template <> int64_t MillisecondsOperator::Operation(timestamp_t input) {
	return Timestamp::GetMilliseconds(input);
}

struct SecondsOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 0;
	}
};

template <> int64_t SecondsOperator::Operation(timestamp_t input) {
	return Timestamp::GetSeconds(input);
}

struct MinutesOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 0;
	}
};

template <> int64_t MinutesOperator::Operation(timestamp_t input) {
	return Timestamp::GetMinutes(input);
}

struct HoursOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return 0;
	}
};

template <> int64_t HoursOperator::Operation(timestamp_t input) {
	return Timestamp::GetHours(input);
}

template <class T> static int64_t extract_element(DatePartSpecifier type, T element) {
	switch (type) {
	case DatePartSpecifier::YEAR:
		return YearOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::MONTH:
		return MonthOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::DAY:
		return DayOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::DECADE:
		return DecadeOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::CENTURY:
		return CenturyOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::MILLENNIUM:
		return MilleniumOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::QUARTER:
		return QuarterOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::DOW:
		return DayOfWeekOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::ISODOW:
		return ISODayOfWeekOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::DOY:
		return DayOfYearOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::WEEK:
		return WeekOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::EPOCH:
		return EpochOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::MICROSECONDS:
		return MicrosecondsOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::MILLISECONDS:
		return MillisecondsOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::SECOND:
		return SecondsOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::MINUTE:
		return MinutesOperator::Operation<T, int64_t>(element);
	case DatePartSpecifier::HOUR:
		return HoursOperator::Operation<T, int64_t>(element);
	default:
		throw NotImplementedException("Specifier type not implemented");
	}
}

struct DatePartOperator {
	template <class TA, class TB, class TR> static inline TR Operation(TA specifier, TB date) {
		return extract_element<TB>(GetDatePartSpecifier(specifier.GetString()), date);
	}
};

template <class OP> static void AddDatePartOperator(BuiltinFunctions &set, string name) {
	ScalarFunctionSet operator_set(name);
	operator_set.AddFunction(
	    ScalarFunction({SQLType::DATE}, SQLType::BIGINT, ScalarFunction::UnaryFunction<date_t, int64_t, OP>));
	operator_set.AddFunction(
	    ScalarFunction({SQLType::TIMESTAMP}, SQLType::BIGINT, ScalarFunction::UnaryFunction<timestamp_t, int64_t, OP>));
	set.AddFunction(operator_set);
}

struct LastDayOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		int32_t yyyy, mm, dd;
		Date::Convert(input, yyyy, mm, dd);
		yyyy += (mm / 12);
		mm %= 12;
		++mm;
		return Date::FromDate(yyyy, mm, 1) - 1;
	}
};

template <> date_t LastDayOperator::Operation(timestamp_t input) {
	return LastDayOperator::Operation<date_t, date_t>(Timestamp::GetDate(input));
}

static string_t s_monthNames[] = {"January", "February", "March",     "April",   "May",      "June",
                                  "July",    "August",   "September", "October", "November", "December"};

struct MonthNameOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return s_monthNames[MonthOperator::Operation<TA, int64_t>(input) - 1];
	}
};

static string_t s_dayNames[] = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};

struct DayNameOperator {
	template <class TA, class TR> static inline TR Operation(TA input) {
		return s_dayNames[DayOfWeekOperator::Operation<TA, int64_t>(input)];
	}
};

void DatePartFun::RegisterFunction(BuiltinFunctions &set) {
	// register the individual operators
	AddDatePartOperator<YearOperator>(set, "year");
	AddDatePartOperator<MonthOperator>(set, "month");
	AddDatePartOperator<DayOperator>(set, "day");
	AddDatePartOperator<DecadeOperator>(set, "decade");
	AddDatePartOperator<CenturyOperator>(set, "century");
	AddDatePartOperator<MilleniumOperator>(set, "millenium");
	AddDatePartOperator<QuarterOperator>(set, "quarter");
	AddDatePartOperator<DayOfWeekOperator>(set, "dayofweek");
	AddDatePartOperator<ISODayOfWeekOperator>(set, "isodow");
	AddDatePartOperator<DayOfYearOperator>(set, "dayofyear");
	AddDatePartOperator<WeekOperator>(set, "week");
	AddDatePartOperator<EpochOperator>(set, "epoch");
	AddDatePartOperator<MicrosecondsOperator>(set, "microsecond");
	AddDatePartOperator<MillisecondsOperator>(set, "millisecond");
	AddDatePartOperator<SecondsOperator>(set, "second");
	AddDatePartOperator<MinutesOperator>(set, "minute");
	AddDatePartOperator<HoursOperator>(set, "hour");

	//  register combinations
	AddDatePartOperator<YearWeekOperator>(set, "yearweek");

	//  register various aliases
	AddDatePartOperator<DayOperator>(set, "dayofmonth");
	AddDatePartOperator<DayOfWeekOperator>(set, "weekday");
	AddDatePartOperator<WeekOperator>(set, "weekofyear"); //  Note that WeekOperator is ISO-8601, not US

	//  register the last_day function
	ScalarFunctionSet last_day("last_day");
	last_day.AddFunction(ScalarFunction({SQLType::DATE}, SQLType::DATE,
	                                    ScalarFunction::UnaryFunction<date_t, date_t, LastDayOperator, true>));
	last_day.AddFunction(ScalarFunction({SQLType::TIMESTAMP}, SQLType::DATE,
	                                    ScalarFunction::UnaryFunction<timestamp_t, date_t, LastDayOperator, true>));
	set.AddFunction(last_day);

	//  register the monthname function
	ScalarFunctionSet monthname("monthname");
	monthname.AddFunction(ScalarFunction({SQLType::DATE}, SQLType::VARCHAR,
	                                     ScalarFunction::UnaryFunction<date_t, string_t, MonthNameOperator, true>));
	monthname.AddFunction(
	    ScalarFunction({SQLType::TIMESTAMP}, SQLType::VARCHAR,
	                   ScalarFunction::UnaryFunction<timestamp_t, string_t, MonthNameOperator, true>));
	set.AddFunction(monthname);

	//  register the dayname function
	ScalarFunctionSet dayname("dayname");
	dayname.AddFunction(ScalarFunction({SQLType::DATE}, SQLType::VARCHAR,
	                                   ScalarFunction::UnaryFunction<date_t, string_t, DayNameOperator, true>));
	dayname.AddFunction(ScalarFunction({SQLType::TIMESTAMP}, SQLType::VARCHAR,
	                                   ScalarFunction::UnaryFunction<timestamp_t, string_t, DayNameOperator, true>));
	set.AddFunction(dayname);

	// finally the actual date_part function
	ScalarFunctionSet date_part("date_part");
	date_part.AddFunction(
	    ScalarFunction({SQLType::VARCHAR, SQLType::DATE}, SQLType::BIGINT,
	                   ScalarFunction::BinaryFunction<string_t, date_t, int64_t, DatePartOperator, true>));
	date_part.AddFunction(
	    ScalarFunction({SQLType::VARCHAR, SQLType::TIMESTAMP}, SQLType::BIGINT,
	                   ScalarFunction::BinaryFunction<string_t, timestamp_t, int64_t, DatePartOperator, true>));
	set.AddFunction(date_part);
	date_part.name = "datepart";
	set.AddFunction(date_part);
}

} // namespace duckdb
