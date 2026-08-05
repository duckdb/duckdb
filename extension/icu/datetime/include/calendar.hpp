//===----------------------------------------------------------------------===//
//                         DuckDB
//
// calendar.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"
#include "timezone.hpp"

namespace duckdb {
namespace datetime {

//! The fields of a calendar. The order matters: several computations resolve a range of fields.
enum CalendarField : uint8_t {
	CAL_ERA = 0,
	CAL_YEAR,
	CAL_MONTH,
	CAL_WEEK_OF_YEAR,
	CAL_WEEK_OF_MONTH,
	CAL_DATE,
	CAL_DAY_OF_YEAR,
	CAL_DAY_OF_WEEK,
	CAL_DAY_OF_WEEK_IN_MONTH,
	CAL_AM_PM,
	CAL_HOUR,
	CAL_HOUR_OF_DAY,
	CAL_MINUTE,
	CAL_SECOND,
	CAL_MILLISECOND,
	CAL_ZONE_OFFSET,
	CAL_DST_OFFSET,
	CAL_YEAR_WOY,
	CAL_DOW_LOCAL,
	CAL_EXTENDED_YEAR,
	CAL_JULIAN_DAY,
	CAL_MILLISECONDS_IN_DAY,
	CAL_IS_LEAP_MONTH,
	CAL_ORDINAL_MONTH,
	CAL_FIELD_COUNT
};

//! Days of the week, as used by CAL_DAY_OF_WEEK
enum CalendarDayOfWeek : uint8_t {
	CAL_SUNDAY = 1,
	CAL_MONDAY = 2,
	CAL_TUESDAY = 3,
	CAL_WEDNESDAY = 4,
	CAL_THURSDAY = 5,
	CAL_FRIDAY = 6,
	CAL_SATURDAY = 7
};

//! Which of the four limits of a field is being asked for
enum class LimitType : uint8_t { MINIMUM = 0, GREATEST_MINIMUM = 1, LEAST_MAXIMUM = 2, MAXIMUM = 3 };

//! A calendar system combined with a time zone: converts between an instant and the fields
//! (year, month, day, ...) that describe it, and performs arithmetic on those fields.
//!
//! Times are whole milliseconds since 1970-01-01 UTC, which is what an int64_t holds exactly
//! over the whole range the calendars cover. The astronomical calculations that the lunisolar
//! calendars need work in fractional milliseconds, and those are doubles, but they are the only
//! place where an instant is not a whole number.
class Calendar {
public:
	virtual ~Calendar() = default;

	//! Creates a calendar of the given type, or nullptr if the type is not known.
	//! An empty type produces the default (Gregorian) calendar.
	static unique_ptr<Calendar> TryCreate(const string &type, unique_ptr<TimeZone> zone);
	//! The names of all supported calendar systems, in lexicographic order
	static const vector<string> &GetAvailableTypes();
	//! The current time, in milliseconds since 1970-01-01 UTC
	static int64_t GetNow();

	virtual unique_ptr<Calendar> Copy() const = 0;
	//! The name of the calendar system, e.g. "gregorian"
	virtual const char *GetType() const = 0;

	virtual void SetTimeZone(unique_ptr<TimeZone> zone) = 0;
	//! Whether the two calendars would produce the same results
	virtual bool Equals(const Calendar &other) const = 0;

	//! Sets the calendar to an instant, discarding all field values
	virtual void SetTime(int64_t millis) = 0;
	//! The instant that the calendar is set to, computing it from the fields if needed
	virtual int64_t GetTime() = 0;
	//! The value of a field, computing the fields from the time if needed
	virtual int32_t Get(CalendarField field) = 0;
	//! Whether the last operation ran into a value that the calendar cannot represent
	virtual bool HasFailed() const = 0;
	//! Sets a field. The time is recomputed the next time it is needed.
	virtual void Set(CalendarField field, int32_t value) = 0;
	//! Adds an amount to a field, keeping the wall clock time invariant for date fields.
	//! Several adds make up one operation, so a failure in any of them is kept until the
	//! next call to SetTime rather than being cleared by the add that follows it.
	virtual void Add(CalendarField field, int32_t amount) = 0;
	//! The number of times the field has to be incremented to reach the target instant
	virtual int32_t FieldDifference(int64_t target, CalendarField field) = 0;

	//! The largest value a field can ever have
	virtual int32_t GetMaximum(CalendarField field) const = 0;
	//! The largest value a field can have given the other fields of the current date
	virtual int32_t GetActualMaximum(CalendarField field) = 0;

	virtual void SetFirstDayOfWeek(int32_t dow) = 0;
	virtual void SetMinimalDaysInFirstWeek(int32_t days) = 0;
};

//! A calendar whose fields are computed lazily. Setting a field records the order in which it was
//! set, and the time is recomputed from the most recently set combination of fields that describes
//! a date. Every calendar system is a subclass that fills in how its fields relate to a Julian day.
class FieldCalendar : public Calendar {
public:
	//! A row of a resolution table lists the fields that all have to be set for the row to
	//! apply, terminated by RESOLVE_STOP. The row resolves to its first field, unless that
	//! one is tagged with RESOLVE_REMAP, in which case the tag holds the field it resolves to.
	static constexpr int32_t RESOLVE_STOP = -1;
	static constexpr int32_t RESOLVE_REMAP = 32;
	//! A group of rows, terminated by a null row. Groups are tried in order.
	using ResolutionGroup = const int32_t *const *;
	//! A table of groups, terminated by a null group
	using ResolutionTable = const ResolutionGroup *;

	//! Whether the years of the first era count backwards, as in the Gregorian BC era
	virtual bool IsEra0CountingBackward() const {
		return false;
	}

	const TimeZone &GetTimeZone() const {
		return *zone;
	}
	void SetTimeZone(unique_ptr<TimeZone> zone_p) override;
	bool Equals(const Calendar &other) const override;

	void SetTime(int64_t millis) override;
	int64_t GetTime() override;
	int32_t Get(CalendarField field) override;
	bool HasFailed() const override {
		return failed;
	}
	void Set(CalendarField field, int32_t value) override;
	void Add(CalendarField field, int32_t amount) override;
	int32_t FieldDifference(int64_t target, CalendarField field) override;

	int32_t GetMaximum(CalendarField field) const override {
		return GetLimit(field, LimitType::MAXIMUM);
	}
	int32_t GetActualMaximum(CalendarField field) override;
	//! The smallest value a field can have given the other fields of the current date
	int32_t GetActualMinimum(CalendarField field);

	int32_t GetFirstDayOfWeek() const {
		return first_day_of_week;
	}
	void SetFirstDayOfWeek(int32_t dow) override;
	int32_t GetMinimalDaysInFirstWeek() const {
		return minimal_days_in_first_week;
	}
	void SetMinimalDaysInFirstWeek(int32_t days) override;

protected:
	explicit FieldCalendar(unique_ptr<TimeZone> zone);
	FieldCalendar(const FieldCalendar &other);

	//! The Julian day of the day before the first day of a month, in this calendar system.
	//! If use_month is false the month is ignored and the day before the year start is returned.
	virtual int64_t HandleComputeMonthStart(int32_t eyear, int32_t month, bool use_month) const = 0;
	//! The number of days in a month of this calendar system, which follows from where the
	//! months start unless the system can describe it more directly
	virtual int32_t HandleGetMonthLength(int32_t eyear, int32_t month) const;
	//! The number of days in a year of this calendar system
	virtual int32_t HandleGetYearLength(int32_t eyear) const;
	//! Computes the era, year, month, day of month, day of year and extended year of a Julian day
	virtual void HandleComputeFields(int32_t julian_day) = 0;
	//! The extended year that the currently set fields describe
	virtual int32_t HandleGetExtendedYear() = 0;
	//! The limit of a field in this calendar system
	virtual int32_t HandleGetLimit(CalendarField field, LimitType type) const = 0;
	//! The month that a year starts with, for calendars that do not start at month 0
	virtual int32_t GetDefaultMonthInYear(int32_t eyear) const {
		return 0;
	}
	//! The day that a month starts with, for calendars that do not start at day 1
	virtual int32_t GetDefaultDayInMonth(int32_t eyear, int32_t month) const {
		return 1;
	}
	//! The extended year that a year and week of the week-based year describe
	int32_t HandleGetExtendedYearFromWeekFields(int32_t year_woy, int32_t woy);
	//! The order in which the fields that describe a date are tried, which a calendar system
	//! whose months are not identified by a number alone overrides
	virtual ResolutionTable GetDatePrecedence() const;

	//! The value of a field without triggering a recomputation
	int32_t InternalGet(CalendarField field) const {
		return fields[field];
	}
	int32_t InternalGet(CalendarField field, int32_t default_value) const {
		return stamp[field] > STAMP_UNSET ? fields[field] : default_value;
	}
	//! Sets a field as part of a computation, which does not affect the resolution order
	void InternalSet(CalendarField field, int32_t value) {
		fields[field] = value;
		stamp[field] = STAMP_INTERNALLY_SET;
	}
	bool IsSet(CalendarField field) const {
		return stamp[field] != STAMP_UNSET;
	}
	//! The month of the current fields, which can be given as a month or as an ordinal month
	int32_t InternalGetMonth() const;
	int32_t InternalGetMonth(int32_t default_value) const;
	//! The more recently set of the two fields
	CalendarField NewerField(CalendarField field, CalendarField alternate) const {
		return stamp[alternate] > stamp[field] ? alternate : field;
	}
	//! The instant the calendar is set to, without recomputing it
	int64_t GetTimeInternal() const {
		return time;
	}

	//! Records that the current operation cannot be represented
	void Fail() const {
		failed = true;
	}
	bool HasFailedInternal() const {
		return failed;
	}
	//! Starts a new operation, which is not affected by whatever went wrong before it
	void ClearFailure() {
		failed = false;
	}

	//! Arithmetic that reports whether the result no longer fits, which the field resolution
	//! has to detect because a date that cannot be represented must not silently wrap around
	static bool TryAdd(int32_t left, int32_t right, int32_t &result);
	static bool TryMultiply(int32_t left, int32_t right, int32_t &result);

	int32_t GetLimit(CalendarField field, LimitType type) const;
	//! The Julian day at which the current fields resolve, ignoring the time of day
	virtual int32_t HandleComputeJulianDay(CalendarField best_field);

	//! Sets up a clone for a GetActualMaximum or GetActualMinimum computation of a field
	void PrepareGetActual(CalendarField field, bool is_minimum);
	//! Makes sure that both the time and the fields are up to date
	void Complete();
	//! A copy of this calendar that the field machinery can be used on
	unique_ptr<FieldCalendar> CopyFields() const;

	//! Tries to work out the difference from the average length of the field, which avoids the
	//! search when the guess turns out to bracket the answer. Returns false if it does not.
	int32_t TryGuessDifference(int64_t start, int64_t target, CalendarField field, int32_t &result);

	//! The operations that make up a public one. Like ICU, which threads a single error code
	//! through an operation, they do nothing once something has gone wrong, so that a failure
	//! is reported instead of a value computed from a broken intermediate state.
	void SetTimeChecked(int64_t millis);
	int64_t GetTimeChecked();
	int32_t GetChecked(CalendarField field);
	//! Calendar systems whose months do not simply follow one another override this
	virtual void AddChecked(CalendarField field, int32_t amount);
	int32_t GetActualMaximumChecked(CalendarField field);
	int32_t GetActualMinimumChecked(CalendarField field);
	//! Clamps a field to the range that it can actually have
	void PinField(CalendarField field);

private:
	//! A field that was never set
	static constexpr int32_t STAMP_UNSET = 0;
	//! A field that was set by a computation rather than by the caller
	static constexpr int32_t STAMP_INTERNALLY_SET = 1;
	//! The first value that is handed out to a field that the caller sets
	static constexpr int32_t STAMP_MINIMUM_USER = 2;

	//! Recomputes the time from the fields
	void ComputeTime();
	//! Recomputes the fields from the time
	void ComputeFields();
	void ComputeWeekFields();
	int64_t ComputeMillisInDay() const;
	//! The total offset that applies to a local time that is split into a day and a time of day
	int32_t ComputeZoneOffset(int64_t millis, int64_t millis_in_day) const;
	int32_t ComputeJulianDay();
	void UpdateTime();
	//! The highest stamp of a range of fields
	int32_t NewestStamp(CalendarField first, CalendarField last, int32_t best_so_far) const;
	//! The field of the resolution table that was most recently set
	CalendarField ResolveFields(ResolutionTable table) const;
	//! The 0-based day of week of the current fields, relative to the first day of the week
	int32_t GetLocalDOW() const;
	//! Walks a field from one limit towards the other until it stops normalizing to itself
	int32_t GetActualHelper(CalendarField field, int32_t start_value, int32_t end_value);
	//! Compacts the stamps once they run out, preserving the order in which fields were set
	void RecalculateStamps();
	//! The week number of a day within a period that starts on the given day of week
	int32_t WeekNumber(int32_t desired_day, int32_t day_of_period, int32_t day_of_week) const;

	static const ResolutionGroup DATE_PRECEDENCE[];
	static const ResolutionGroup YEAR_PRECEDENCE[];
	static const ResolutionGroup DOW_PRECEDENCE[];
	static const ResolutionGroup MONTH_PRECEDENCE[];

protected:
	//! The proleptic Gregorian fields of the current Julian day, which every calendar
	//! system is computed from
	int32_t gregorian_year;
	int8_t gregorian_month;
	int8_t gregorian_dom;
	int16_t gregorian_doy;

private:
	unique_ptr<TimeZone> zone;
	//! The instant the calendar is set to, valid if time_set
	int64_t time;
	int32_t fields[CAL_FIELD_COUNT];
	//! For every field, when it was set relative to the other fields
	int32_t stamp[CAL_FIELD_COUNT];
	int32_t next_stamp;
	bool time_set;
	bool fields_set;
	bool all_fields_set;
	//! Whether the fields still have to be computed from the time
	bool fields_virtually_set;
	//! Whether the last operation ran into a value that cannot be represented
	mutable bool failed;
	int32_t first_day_of_week;
	int32_t minimal_days_in_first_week;
};

} // namespace datetime
} // namespace duckdb
