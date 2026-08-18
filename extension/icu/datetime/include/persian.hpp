//===----------------------------------------------------------------------===//
//                         DuckDB
//
// persian.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "calendar.hpp"

namespace duckdb {
namespace datetime {

//! The Persian (Solar Hijri) calendar: six months of 31 days, five of 30, and a last month of
//! 29 days that gains a day in a leap year. Leap years follow a 33 year cycle, with a list of
//! corrections for the years in which the astronomical rule disagrees with it.
class PersianCalendar : public FieldCalendar {
public:
	explicit PersianCalendar(unique_ptr<TimeZone> zone) : FieldCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "persian";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new PersianCalendar(*this));
	}

protected:
	PersianCalendar(const PersianCalendar &other) = default;

	int64_t HandleComputeMonthStart(int32_t eyear, int32_t month, bool use_month) const override;
	int32_t HandleGetMonthLength(int32_t eyear, int32_t month) const override;
	int32_t HandleGetYearLength(int32_t eyear) const override;
	void HandleComputeFields(int32_t julian_day) override;
	int32_t HandleGetExtendedYear() override;
	int32_t HandleGetLimit(CalendarField field, LimitType type) const override;
};

//! The Indian national (Saka) calendar: a first month that follows the Gregorian leap years and
//! is 30 or 31 days long, then five months of 31 days and six of 30.
class IndianCalendar : public FieldCalendar {
public:
	explicit IndianCalendar(unique_ptr<TimeZone> zone) : FieldCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "indian";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new IndianCalendar(*this));
	}

protected:
	IndianCalendar(const IndianCalendar &other) = default;

	int64_t HandleComputeMonthStart(int32_t eyear, int32_t month, bool use_month) const override;
	int32_t HandleGetMonthLength(int32_t eyear, int32_t month) const override;
	int32_t HandleGetYearLength(int32_t eyear) const override;
	void HandleComputeFields(int32_t julian_day) override;
	int32_t HandleGetExtendedYear() override;
	int32_t HandleGetLimit(CalendarField field, LimitType type) const override;
};

} // namespace datetime
} // namespace duckdb
