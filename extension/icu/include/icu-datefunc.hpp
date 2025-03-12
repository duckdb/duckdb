//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu-datefunc.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

#include "duckdb/common/enums/date_part_specifier.hpp"
#include "duckdb/planner/expression/bound_function_expression.hpp"
#include "unicode/calendar.h"

namespace duckdb {

struct ICUDateFunc {
	using CalendarPtr = duckdb::unique_ptr<icu::Calendar>;

	struct BindData : public FunctionData {
		explicit BindData(ClientContext &context);
		BindData(const string &tz_setting, const string &cal_setting);
		BindData(const BindData &other);

		string tz_setting;
		string cal_setting;
		CalendarPtr calendar;

		bool Equals(const FunctionData &other_p) const override;
		duckdb::unique_ptr<FunctionData> Copy() const override;

		void InitCalendar();
	};

	struct CastData : public BoundCastData {
		explicit CastData(duckdb::unique_ptr<FunctionData> info_p) : info(std::move(info_p)) {
		}

		duckdb::unique_ptr<BoundCastData> Copy() const override {
			return make_uniq<CastData>(info->Copy());
		}

		duckdb::unique_ptr<FunctionData> info;
	};

	//! Binds a default calendar object for use by the function
	static duckdb::unique_ptr<FunctionData> Bind(ClientContext &context, ScalarFunction &bound_function,
	                                             vector<duckdb::unique_ptr<Expression>> &arguments);

	//! Tries to set the time zone for the calendar and returns false if it is not valid.
	static bool TrySetTimeZone(icu::Calendar *calendar, const string_t &tz_id);
	//! Sets the time zone for the calendar. Throws if it is not valid
	static void SetTimeZone(icu::Calendar *calendar, const string_t &tz_id);
	//! Gets the timestamp from the calendar, throwing if it is not in range.
	static bool TryGetTime(icu::Calendar *calendar, uint64_t micros, timestamp_t &result);
	//! Gets the timestamp from the calendar, throwing if it is not in range.
	static timestamp_t GetTime(icu::Calendar *calendar, uint64_t micros = 0);
	//! Gets the timestamp from the calendar, assuming it is in range.
	static timestamp_t GetTimeUnsafe(icu::Calendar *calendar, uint64_t micros = 0);
	//! Sets the calendar to the timestamp, returning the unused µs part
	static uint64_t SetTime(icu::Calendar *calendar, timestamp_t date);
	//! Extracts the field from the calendar
	static int32_t ExtractField(icu::Calendar *calendar, UCalendarDateFields field);
	//! Subtracts the field of the given date from the calendar
	static int32_t SubtractField(icu::Calendar *calendar, UCalendarDateFields field, timestamp_t end_date);
	//! Adds the timestamp and the interval using the calendar
	static timestamp_t Add(icu::Calendar *calendar, timestamp_t timestamp, interval_t interval);
	//! Subtracts the interval from the timestamp using the calendar
	static timestamp_t Sub(icu::Calendar *calendar, timestamp_t timestamp, interval_t interval);
	//! Subtracts the latter timestamp from the former timestamp using the calendar
	static interval_t Sub(icu::Calendar *calendar, timestamp_t end_date, timestamp_t start_date);
	//! Pulls out the bin values from the timestamp assuming it is an instant,
	//! constructs an ICU timestamp, and then converts that back to a DuckDB instant
	//! Adding offset doesn't really work around DST because the bin values are ambiguous
	static timestamp_t FromNaive(icu::Calendar *calendar, timestamp_t naive);

	//! Truncates the calendar time to the given part precision
	typedef void (*part_trunc_t)(icu::Calendar *calendar, uint64_t &micros);
	static part_trunc_t TruncationFactory(DatePartSpecifier part);
	static timestamp_t CurrentMidnight(icu::Calendar *calendar, ExpressionState &state);

	//! Subtracts the two times at the given part precision
	typedef int64_t (*part_sub_t)(icu::Calendar *calendar, timestamp_t start_date, timestamp_t end_date);
	static part_sub_t SubtractFactory(DatePartSpecifier part);
};

} // namespace duckdb
