//===----------------------------------------------------------------------===//
//                         DuckDB
//
// gregorian.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "calendar.hpp"
#include "grego.hpp"

namespace duckdb {
namespace datetime {

//! The proleptic Gregorian calendar: the Gregorian rules are applied to all dates, including the
//! ones that historically predate the calendar. This is what Postgres assumes as well.
class GregorianCalendar : public FieldCalendar {
public:
	//! The eras of the Gregorian calendar
	static constexpr int32_t BC = 0;
	static constexpr int32_t AD = 1;
	//! The Julian day and the year in which the Gregorian calendar took effect (1582-10-15).
	//! Dates before it are given in the Julian calendar.
	static constexpr int32_t PAPAL_CUTOVER_JULIAN_DAY = 2299161;
	static constexpr int32_t PAPAL_CUTOVER_YEAR = 1582;

	//! A proleptic calendar applies the Gregorian rules to every date, including the ones that
	//! historically predate the calendar. Postgres assumes a proleptic calendar, and so does the
	//! extension, but the calendars that are derived from this one keep the historical cutover.
	explicit GregorianCalendar(unique_ptr<TimeZone> zone, bool proleptic = false)
	    : FieldCalendar(std::move(zone)),
	      cutover_julian_day(proleptic ? NumericLimits<int32_t>::Minimum() : PAPAL_CUTOVER_JULIAN_DAY),
	      cutover_year(proleptic ? NumericLimits<int32_t>::Minimum() : PAPAL_CUTOVER_YEAR), is_gregorian(true),
	      invert_gregorian(false) {
	}

	const char *GetType() const override {
		return "gregorian";
	}
	bool IsEra0CountingBackward() const override {
		return true;
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new GregorianCalendar(*this));
	}

protected:
	GregorianCalendar(const GregorianCalendar &other) = default;

	//! Leap years follow the Julian rules before the cutover and the Gregorian ones after it
	bool IsLeapYear(int32_t eyear) const {
		return eyear >= cutover_year ? Grego::IsLeapYear(eyear) : (eyear & 0x3) == 0;
	}

	int64_t HandleComputeMonthStart(int32_t eyear, int32_t month, bool use_month) const override;
	int32_t HandleGetMonthLength(int32_t eyear, int32_t month) const override;
	int32_t HandleGetYearLength(int32_t eyear) const override;
	void HandleComputeFields(int32_t julian_day) override;
	int32_t HandleGetExtendedYear() override;
	int32_t HandleGetLimit(CalendarField field, LimitType type) const override;
	int32_t HandleComputeJulianDay(CalendarField best_field) override;

	//! The first Julian day of the Gregorian calendar, and the year that holds it
	int32_t cutover_julian_day;
	int32_t cutover_year;
	//! Whether the year that is currently being computed uses the Gregorian rules
	mutable bool is_gregorian;
	//! Whether that decision is deliberately inverted to reach across the cutover
	mutable bool invert_gregorian;
};

//! The Buddhist calendar: the Gregorian calendar with the years counted from 544 BC, in a
//! single era that does not count backwards.
class BuddhistCalendar : public GregorianCalendar {
public:
	//! The first Gregorian year of the Buddhist era
	static constexpr int32_t ERA_START = -543;
	//! The only era
	static constexpr int32_t BE = 0;

	explicit BuddhistCalendar(unique_ptr<TimeZone> zone) : GregorianCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "buddhist";
	}
	bool IsEra0CountingBackward() const override {
		return false;
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new BuddhistCalendar(*this));
	}

protected:
	BuddhistCalendar(const BuddhistCalendar &other) = default;

	void HandleComputeFields(int32_t julian_day) override;
	int32_t HandleGetExtendedYear() override;
	int32_t HandleGetLimit(CalendarField field, LimitType type) const override;
};

//! The Republic of China calendar: the Gregorian calendar with the years counted from 1912,
//! in an era before and an era after that year.
class ROCCalendar : public GregorianCalendar {
public:
	//! The Gregorian year before the first year of the Minguo era
	static constexpr int32_t ERA_START = 1911;
	static constexpr int32_t BEFORE_MINGUO = 0;
	static constexpr int32_t MINGUO = 1;

	explicit ROCCalendar(unique_ptr<TimeZone> zone) : GregorianCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "roc";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new ROCCalendar(*this));
	}

protected:
	ROCCalendar(const ROCCalendar &other) = default;

	void HandleComputeFields(int32_t julian_day) override;
	int32_t HandleGetExtendedYear() override;
	int32_t HandleGetLimit(CalendarField field, LimitType type) const override;
};

//! The ISO 8601 calendar: the Gregorian calendar with weeks that start on Monday and that
//! belong to the year holding the majority of their days.
class ISO8601Calendar : public GregorianCalendar {
public:
	//	Like the Gregorian calendar, this one is made proleptic by the extension
	explicit ISO8601Calendar(unique_ptr<TimeZone> zone) : GregorianCalendar(std::move(zone), true) {
		SetFirstDayOfWeek(CAL_MONDAY);
		SetMinimalDaysInFirstWeek(4);
	}

	const char *GetType() const override {
		return "iso8601";
	}
	bool IsEra0CountingBackward() const override {
		return false;
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new ISO8601Calendar(*this));
	}

protected:
	ISO8601Calendar(const ISO8601Calendar &other) = default;
};

} // namespace datetime
} // namespace duckdb
