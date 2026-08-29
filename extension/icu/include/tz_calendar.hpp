//===----------------------------------------------------------------------===//
//                         DuckDB
//
// tz_calendar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "calendar.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

using datetime::Calendar;
using datetime::CalendarField;
using datetime::LocalOption;
using datetime::TimeZone;

using datetime::CAL_AM_PM;
using datetime::CAL_DATE;
using datetime::CAL_DAY_OF_WEEK;
using datetime::CAL_DAY_OF_WEEK_IN_MONTH;
using datetime::CAL_DAY_OF_YEAR;
using datetime::CAL_DOW_LOCAL;
using datetime::CAL_DST_OFFSET;
using datetime::CAL_ERA;
using datetime::CAL_EXTENDED_YEAR;
using datetime::CAL_HOUR;
using datetime::CAL_HOUR_OF_DAY;
using datetime::CAL_JULIAN_DAY;
using datetime::CAL_MILLISECOND;
using datetime::CAL_MILLISECONDS_IN_DAY;
using datetime::CAL_MINUTE;
using datetime::CAL_MONTH;
using datetime::CAL_ORDINAL_MONTH;
using datetime::CAL_SECOND;
using datetime::CAL_WEEK_OF_MONTH;
using datetime::CAL_WEEK_OF_YEAR;
using datetime::CAL_YEAR;
using datetime::CAL_YEAR_WOY;
using datetime::CAL_ZONE_OFFSET;

using datetime::CAL_MONDAY;
using datetime::CAL_SUNDAY;

//! The first month of the year, which is 0-based like all months
static constexpr int32_t CAL_JANUARY = 0;

using CalendarPtr = duckdb::unique_ptr<Calendar>;

struct TZCalendar {
	TZCalendar(Calendar &calendar_p, const string &cal_setting)
	    : calendar(calendar_p.Copy()),
	      is_gregorian(cal_setting.empty() || StringUtil::CIEquals(cal_setting, "gregorian")),
	      supports_intervals(calendar->GetMaximum(CAL_MONTH) < 12) { // 0-based
	}

	Calendar *GetCalendar() {
		return calendar.get();
	}
	bool IsGregorian() const {
		return is_gregorian;
	}
	bool SupportsIntervals() const {
		return supports_intervals;
	}

	CalendarPtr calendar;
	const bool is_gregorian;
	const bool supports_intervals;
};

} // namespace duckdb
