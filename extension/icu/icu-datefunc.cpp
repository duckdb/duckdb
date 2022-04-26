#include "include/icu-datefunc.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

ICUDateFunc::BindData::BindData(const BindData &other) : calendar(other.calendar->clone()) {
}

ICUDateFunc::BindData::BindData(ClientContext &context) {
	Value tz_value;
	if (context.TryGetCurrentSetting("TimeZone", tz_value)) {
		tz_setting = tz_value.ToString();
	}
	auto tz = icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(tz_setting)));

	string cal_id("@calendar=");
	Value cal_value;
	if (context.TryGetCurrentSetting("Calendar", cal_value)) {
		cal_setting = cal_value.ToString();
		cal_id += cal_setting;
	} else {
		cal_id += "gregorian";
	}

	icu::Locale locale(cal_id.c_str());

	UErrorCode success = U_ZERO_ERROR;
	calendar.reset(icu::Calendar::createInstance(tz, locale, success));
	if (U_FAILURE(success)) {
		throw Exception("Unable to create ICU calendar.");
	}
}

bool ICUDateFunc::BindData::Equals(const FunctionData &other_p) const {
	auto &other = (const ICUDateFunc::BindData &)other_p;
	return *calendar == *other.calendar;
}

unique_ptr<FunctionData> ICUDateFunc::BindData::Copy() const {
	return make_unique<BindData>(*this);
}

unique_ptr<FunctionData> ICUDateFunc::Bind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	return make_unique<BindData>(context);
}

void ICUDateFunc::SetTimeZone(icu::Calendar *calendar, const string_t &tz_id) {
	auto tz = icu_66::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(tz_id.GetString())));
	calendar->adoptTimeZone(tz);
}

timestamp_t ICUDateFunc::GetTimeUnsafe(icu::Calendar *calendar, uint64_t micros) {
	// Extract the new time
	UErrorCode status = U_ZERO_ERROR;
	const auto millis = int64_t(calendar->getTime(status));
	if (U_FAILURE(status)) {
		throw Exception("Unable to get ICU calendar time.");
	}
	return timestamp_t(millis * Interval::MICROS_PER_MSEC + micros);
}

timestamp_t ICUDateFunc::GetTime(icu::Calendar *calendar, uint64_t micros) {
	// Extract the new time
	UErrorCode status = U_ZERO_ERROR;
	auto millis = int64_t(calendar->getTime(status));
	if (U_FAILURE(status)) {
		throw Exception("Unable to get ICU calendar time.");
	}

	// UDate is a double, so it can't overflow (it just loses accuracy), but converting back to µs can.
	millis = MultiplyOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(millis, Interval::MICROS_PER_MSEC);
	millis = AddOperatorOverflowCheck::Operation<int64_t, int64_t, int64_t>(millis, micros);

	// Now make sure the value is in range
	date_t d;
	dtime_t t;
	Timestamp::Convert(timestamp_t(millis), d, t);

	return timestamp_t(millis);
}

uint64_t ICUDateFunc::SetTime(icu::Calendar *calendar, timestamp_t date) {
	int64_t millis = date.value / Interval::MICROS_PER_MSEC;
	uint64_t micros = date.value % Interval::MICROS_PER_MSEC;

	const auto udate = UDate(millis);
	UErrorCode status = U_ZERO_ERROR;
	calendar->setTime(udate, status);
	if (U_FAILURE(status)) {
		throw Exception("Unable to set ICU calendar time.");
	}
	return micros;
}

int32_t ICUDateFunc::ExtractField(icu::Calendar *calendar, UCalendarDateFields field) {
	UErrorCode status = U_ZERO_ERROR;
	const auto result = calendar->get(field, status);
	if (U_FAILURE(status)) {
		throw Exception("Unable to extract ICU calendar part.");
	}
	return result;
}

int64_t ICUDateFunc::SubtractField(icu::Calendar *calendar, UCalendarDateFields field, timestamp_t end_date) {
	// ICU triggers the address sanitiser because it tries to left shift a negative value
	// when start_date > end_date. To avoid this, we swap the values and negate the result.
	const auto start_date = GetTimeUnsafe(calendar);
	if (start_date > end_date) {
		SetTime(calendar, end_date);
		return -SubtractField(calendar, field, start_date);
	}

	const int64_t millis = end_date.value / Interval::MICROS_PER_MSEC;
	const auto when = UDate(millis);
	UErrorCode status = U_ZERO_ERROR;
	auto sub = calendar->fieldDifference(when, field, status);
	if (U_FAILURE(status)) {
		throw Exception("Unable to subtract ICU calendar part.");
	}
	return sub;
}

} // namespace duckdb
