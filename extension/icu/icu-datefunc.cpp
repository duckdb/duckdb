#include "include/icu-datefunc.hpp"

#include "duckdb/main/client_context.hpp"

namespace duckdb {

ICUDateFunc::BindData::BindData(const BindData &other) : calendar(other.calendar->clone()) {
}

ICUDateFunc::BindData::BindData(ClientContext &context) {
	Value tz_value;
	string tz_id;
	if (context.TryGetCurrentSetting("TimeZone", tz_value)) {
		tz_id = tz_value.ToString();
	}
	auto tz = icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(tz_id)));

	UErrorCode success = U_ZERO_ERROR;
	calendar.reset(icu::Calendar::createInstance(tz, success));
	if (U_FAILURE(success)) {
		throw Exception("Unable to create ICU calendar.");
	}
}

unique_ptr<FunctionData> ICUDateFunc::BindData::Copy() {
	return make_unique<BindData>(*this);
}

unique_ptr<FunctionData> ICUDateFunc::Bind(ClientContext &context, ScalarFunction &bound_function,
                                           vector<unique_ptr<Expression>> &arguments) {
	return make_unique<BindData>(context);
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

} // namespace duckdb
