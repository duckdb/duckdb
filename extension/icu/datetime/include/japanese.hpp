//===----------------------------------------------------------------------===//
//                         DuckDB
//
// japanese.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "gregorian.hpp"

namespace duckdb {
namespace datetime {

//! The Gregorian date that a Japanese era starts on
struct JapaneseEra {
	int16_t year;
	int8_t month;
	int8_t day;
};

//! The eras, in ascending order, from Taika to the current one
extern const JapaneseEra JAPANESE_ERAS[];
extern const idx_t JAPANESE_ERA_COUNT;

//! The Japanese calendar: the Gregorian calendar with the years counted from the start of the
//! era of the reigning emperor. Because the eras start part way through a year, the first and
//! the last year of an era are shorter than a whole year.
class JapaneseCalendar : public GregorianCalendar {
public:
	explicit JapaneseCalendar(unique_ptr<TimeZone> zone) : GregorianCalendar(std::move(zone)) {
	}

	const char *GetType() const override {
		return "japanese";
	}
	bool IsEra0CountingBackward() const override {
		return false;
	}
	unique_ptr<Calendar> Copy() const override {
		return unique_ptr<Calendar>(new JapaneseCalendar(*this));
	}
	int32_t GetActualMaximum(CalendarField field) override;

	//! The era that the given Gregorian date falls in
	static int32_t GetEra(int32_t year, int32_t month, int32_t day);
	//! The era that is in effect now, which is what an unset era defaults to
	static int32_t GetCurrentEra();

protected:
	JapaneseCalendar(const JapaneseCalendar &other) = default;

	//! The era of the current fields, defaulting to the one in effect now
	int32_t InternalGetEra() const {
		return InternalGet(CAL_ERA, GetCurrentEra());
	}

	void HandleComputeFields(int32_t julian_day) override;
	int32_t HandleGetExtendedYear() override;
	int32_t HandleGetLimit(CalendarField field, LimitType type) const override;
	int32_t GetDefaultMonthInYear(int32_t eyear) const override;
	int32_t GetDefaultDayInMonth(int32_t eyear, int32_t month) const override;
};

} // namespace datetime
} // namespace duckdb
