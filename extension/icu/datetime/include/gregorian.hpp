//===----------------------------------------------------------------------===//
//                         DuckDB
//
// gregorian.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "calendar.hpp"

namespace duckdb {
namespace datetime {

//! The proleptic Gregorian calendar: the Gregorian rules are applied to all dates, including the
//! ones that historically predate the calendar. This is what Postgres assumes as well.
class GregorianCalendar : public FieldCalendar {
public:
	//! The eras of the Gregorian calendar
	static constexpr int32_t BC = 0;
	static constexpr int32_t AD = 1;

	explicit GregorianCalendar(unique_ptr<TimeZone> zone) : FieldCalendar(std::move(zone)) {
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

	int64_t HandleComputeMonthStart(int32_t eyear, int32_t month, bool use_month) const override;
	int32_t HandleGetMonthLength(int32_t eyear, int32_t month) const override;
	int32_t HandleGetYearLength(int32_t eyear) const override;
	void HandleComputeFields(int32_t julian_day) override;
	int32_t HandleGetExtendedYear() override;
	int32_t HandleGetLimit(CalendarField field, LimitType type) const override;
};

} // namespace datetime
} // namespace duckdb
