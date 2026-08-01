//===----------------------------------------------------------------------===//
//                         DuckDB
//
// icu_calendar.cpp
//
// Temporary: the calendar systems that have not been reimplemented yet are still backed by ICU.
// This file shrinks with every calendar that is ported and is removed once ICU is gone.
//
//===----------------------------------------------------------------------===//

#include "calendar.hpp"
#include "duckdb/common/string_util.hpp"
#include "unicode/calendar.h"
#include "unicode/timezone.h"
#include "unicode/ucal.h"

namespace duckdb {
namespace datetime {

namespace {

//! A calendar system that is still provided by ICU
class ICUCalendar : public Calendar {
public:
	ICUCalendar(string type_p, unique_ptr<TimeZone> zone_p, duckdb::unique_ptr<icu::Calendar> calendar_p)
	    : type(std::move(type_p)), zone(std::move(zone_p)), calendar(std::move(calendar_p)), failed(false) {
	}

	unique_ptr<Calendar> Copy() const override {
		auto zone_copy = zone->Copy();
		duckdb::unique_ptr<icu::Calendar> calendar_copy(calendar->clone());
		return make_uniq<ICUCalendar>(type, std::move(zone_copy), std::move(calendar_copy));
	}

	const char *GetType() const override {
		return type.c_str();
	}

	void SetTimeZone(unique_ptr<TimeZone> zone_p) override {
		zone = std::move(zone_p);
		calendar->adoptTimeZone(
		    icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(zone->GetId()))));
	}

	bool Equals(const Calendar &other) const override {
		if (!StringUtil::Equals(GetType(), other.GetType())) {
			return false;
		}
		return calendar->isEquivalentTo(*static_cast<const ICUCalendar &>(other).calendar);
	}

	void SetTime(double millis) override {
		UErrorCode status = U_ZERO_ERROR;
		calendar->setTime(millis, status);
		failed = U_FAILURE(status);
	}

	double GetTime() override {
		UErrorCode status = U_ZERO_ERROR;
		const auto result = double(calendar->getTime(status));
		failed = U_FAILURE(status);
		return result;
	}

	int32_t Get(CalendarField field) override {
		UErrorCode status = U_ZERO_ERROR;
		const auto result = calendar->get(UCalendarDateFields(field), status);
		failed = U_FAILURE(status);
		return result;
	}

	bool HasFailed() const override {
		return failed;
	}

	void Set(CalendarField field, int32_t value) override {
		calendar->set(UCalendarDateFields(field), value);
	}

	void Add(CalendarField field, int32_t amount) override {
		UErrorCode status = U_ZERO_ERROR;
		calendar->add(UCalendarDateFields(field), amount, status);
		failed = U_FAILURE(status);
	}

	int32_t FieldDifference(double target, CalendarField field) override {
		UErrorCode status = U_ZERO_ERROR;
		const auto result = calendar->fieldDifference(target, UCalendarDateFields(field), status);
		failed = U_FAILURE(status);
		return result;
	}

	int32_t GetMaximum(CalendarField field) const override {
		return calendar->getMaximum(UCalendarDateFields(field));
	}

	int32_t GetActualMaximum(CalendarField field) override {
		UErrorCode status = U_ZERO_ERROR;
		const auto result = calendar->getActualMaximum(UCalendarDateFields(field), status);
		failed = U_FAILURE(status);
		return result;
	}

	void SetFirstDayOfWeek(int32_t dow) override {
		calendar->setFirstDayOfWeek(UCalendarDaysOfWeek(dow));
	}

	void SetMinimalDaysInFirstWeek(int32_t days) override {
		calendar->setMinimalDaysInFirstWeek(uint8_t(days));
	}

private:
	string type;
	unique_ptr<TimeZone> zone;
	duckdb::unique_ptr<icu::Calendar> calendar;
	bool failed;
};

} // namespace

unique_ptr<Calendar> TryCreateICUCalendar(const string &type, unique_ptr<TimeZone> zone) {
	string locale_id("@calendar=");
	locale_id += type;
	icu::Locale locale(locale_id.c_str());

	UErrorCode status = U_ZERO_ERROR;
	duckdb::unique_ptr<icu::Calendar> calendar(icu::Calendar::createInstance(
	    icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(zone->GetId()))), locale, status));
	if (U_FAILURE(status) || type != calendar->getType()) {
		return nullptr;
	}
	//	Postgres always assumes times are given in the proleptic Gregorian calendar, so the
	//	Gregorian change is moved to the minimum date. This fails for the non-Gregorian
	//	calendars, which is what ICU does as well.
	ucal_setGregorianChange(reinterpret_cast<UCalendar *>(calendar.get()), U_DATE_MIN, &status);
	return make_uniq<ICUCalendar>(type, std::move(zone), std::move(calendar));
}

vector<string> GetICUCalendarTypes() {
	vector<string> result;
	UErrorCode status = U_ZERO_ERROR;
	duckdb::unique_ptr<icu::StringEnumeration> types(
	    icu::Calendar::getKeywordValuesForLocale("calendar", icu::Locale::getDefault(), false, status));
	if (U_FAILURE(status) || !types) {
		return result;
	}
	for (;;) {
		auto type = types->snext(status);
		if (U_FAILURE(status) || !type) {
			break;
		}
		std::string utf8;
		type->toUTF8String(utf8);
		result.emplace_back(utf8);
	}
	return result;
}

} // namespace datetime
} // namespace duckdb
