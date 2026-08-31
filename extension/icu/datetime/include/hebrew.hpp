//===----------------------------------------------------------------------===//
//                         DuckDB
//
// hebrew.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "calendar.hpp"

namespace duckdb {
namespace datetime {

//! The Hebrew calendar: a lunisolar calendar in which seven years of every nineteen gain a leap
//! month, and in which the length of two of the months varies so that the year does not start on
//! a day the festivals may not fall on.
class HebrewCalendar : public FieldCalendar {
public:
	//! The months, which are always numbered 0..12 - the leap month is simply absent from a
	//! year that does not have one
	static constexpr int32_t TISHRI = 0;
	static constexpr int32_t HESHVAN = 1;
	static constexpr int32_t KISLEV = 2;
	static constexpr int32_t ADAR_1 = 5;
	static constexpr int32_t ELUL = 12;
	//! Nineteen years hold 235 months
	static constexpr int32_t MONTHS_IN_CYCLE = 235;
	static constexpr int32_t YEARS_IN_CYCLE = 19;

	explicit HebrewCalendar(unique_ptr<TimeZone> zone) : FieldCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "hebrew";
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new HebrewCalendar(*this));
	}

	//! Seven years of every nineteen have a leap month
	static bool IsLeapYear(int32_t year);

protected:
	HebrewCalendar(const HebrewCalendar &other) = default;

	//! Adding months has to step over the leap month in the years that do not have one
	void AddChecked(CalendarField field, int32_t amount) override;

	int64_t HandleComputeMonthStart(int32_t eyear, int32_t month, bool use_month) const override;
	int32_t HandleGetMonthLength(int32_t eyear, int32_t month) const override;
	int32_t HandleGetYearLength(int32_t eyear) const override;
	void HandleComputeFields(int32_t julian_day) override;
	int32_t HandleGetExtendedYear() override;
	int32_t HandleGetLimit(CalendarField field, LimitType type) const override;
};

} // namespace datetime
} // namespace duckdb
