//===----------------------------------------------------------------------===//
//                         DuckDB
//
// tz_calendar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "icu-datefunc.hpp"
#include "duckdb/common/string_util.hpp"

namespace duckdb {

using CalendarPtr = duckdb::unique_ptr<icu::Calendar>;

struct TZCalendar {
	TZCalendar(icu::Calendar &calendar_p, const string &cal_setting)
	    : calendar(CalendarPtr(calendar_p.clone())),
	      is_gregorian(cal_setting.empty() || StringUtil::CIEquals(cal_setting, "gregorian")) {
	}

	icu::Calendar *GetICUCalendar() {
		return calendar.get();
	}
	bool IsGregorian() const {
		return is_gregorian;
	}

	CalendarPtr calendar;
	bool is_gregorian;
};

} // namespace duckdb
