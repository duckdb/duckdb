//===----------------------------------------------------------------------===//
//                         DuckDB
//
// chinese.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "calendar.hpp"

namespace duckdb {
namespace datetime {

//! The Chinese calendar: a lunisolar calendar in which a month starts on the day of a new moon
//! and the year on the second new moon after the winter solstice, with a leap month inserted in
//! the years that hold thirteen new moons between two solstices.
//!
//! The years are counted in sixty year cycles, which the era field holds, with the year within
//! the cycle in the year field.
class ChineseCalendar : public FieldCalendar {
public:
	explicit ChineseCalendar(unique_ptr<TimeZone> zone)
	    : FieldCalendar(std::move(zone)), has_leap_month_between_solstices(false) {
	}

	const char *GetType() const override {
		return "chinese";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new ChineseCalendar(*this));
	}
	int32_t GetActualMaximum(CalendarField field) override;

protected:
	ChineseCalendar(const ChineseCalendar &other) = default;

	//! The offset of the zone the astronomical calculations are done in, which is the zone of
	//! the country whose calendar it is rather than the zone of the session
	virtual int32_t GetAstronomerOffset(double millis) const;
	//! The caches are per calendar system, because the two systems compute different values
	virtual idx_t GetSettingIndex() const {
		return 0;
	}

	//! Adding months has to follow the new moons rather than a fixed number of days
	void AddChecked(CalendarField field, int32_t amount) override;

	int64_t HandleComputeMonthStart(int32_t eyear, int32_t month, bool use_month) const override;
	int32_t HandleGetMonthLength(int32_t eyear, int32_t month) const override;
	void HandleComputeFields(int32_t julian_day) override;
	int32_t HandleGetExtendedYear() override;
	int32_t HandleGetLimit(CalendarField field, LimitType type) const override;
	ResolutionTable GetDatePrecedence() const override;

	//! What a date resolves to in this calendar system
	struct MonthInfo {
		int32_t month;
		int32_t ordinal_month;
		int32_t this_moon;
		bool is_leap_month;
		bool has_leap_month_between_solstices;
	};

	//! Converts between local days in the astronomer zone and instants
	double DaysToMillis(double days) const;
	double MillisToDays(double millis) const;
	//! The local day of the new moon nearest to a day, searching forwards or backwards
	int32_t NewMoonNear(double days, bool after) const;
	//! The local day of the winter solstice of a Gregorian year
	int32_t WinterSolstice(int32_t gyear) const;
	//! The local day the Chinese year that falls in a Gregorian year starts on
	int32_t NewYear(int32_t gyear) const;
	//! Which of the twelve major solar terms a day falls in
	int32_t GetMajorSolarTerm(int32_t days) const;
	//! Whether a month holds no major solar term, which is what makes it a leap month
	bool HasNoMajorSolarTerm(int32_t new_moon) const;
	//! Whether there is a leap month in the range of months
	bool IsLeapMonthBetween(int32_t new_moon1, int32_t new_moon2) const;
	//! Resolves a local day into the month it belongs to
	MonthInfo ComputeMonthInfo(int32_t gyear, int32_t days) const;
	//! The Julian day of the start of a month, taking the leap month into account
	int64_t ComputeMonthStart(int32_t eyear, int32_t month, bool is_leap_month) const;
	int32_t GetMonthLength(int32_t eyear, int32_t month, bool is_leap_month) const;
	//! Moves the calendar to the same day of a month a number of months away
	void OffsetMonth(int32_t new_moon, int32_t dom, int32_t delta);

	//! Whether the year the calendar is set to holds a leap month
	bool has_leap_month_between_solstices;
};

//! The Korean calendar, which follows the same rules as the Chinese one but does its
//! astronomical calculations in Korean local time
class DangiCalendar : public ChineseCalendar {
public:
	explicit DangiCalendar(unique_ptr<TimeZone> zone) : ChineseCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "dangi";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new DangiCalendar(*this));
	}

protected:
	DangiCalendar(const DangiCalendar &other) = default;

	int32_t GetAstronomerOffset(double millis) const override;
	idx_t GetSettingIndex() const override {
		return 1;
	}
};

} // namespace datetime
} // namespace duckdb
