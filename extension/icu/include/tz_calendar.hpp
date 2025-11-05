//===----------------------------------------------------------------------===//
//                         DuckDB
//
// tz_calendar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "unicode/calendar.h"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

using CalendarPtr = duckdb::unique_ptr<icu::Calendar>;

struct TZCalendar {
	TZCalendar(icu::Calendar &calendar_p, const string &cal_setting)
	    : calendar(CalendarPtr(calendar_p.clone())),
	      is_gregorian(cal_setting.empty() || StringUtil::CIEquals(cal_setting, "gregorian")),
	      supports_intervals(calendar->getMaximum(UCAL_MONTH) < 12) { // 0-based
	}

	icu::Calendar *GetICUCalendar() {
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
