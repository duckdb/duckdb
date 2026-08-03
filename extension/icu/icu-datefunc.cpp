#include "include/icu-datefunc.hpp"

#include "duckdb/main/client_context.hpp"
#include "duckdb/common/operator/add.hpp"
#include "duckdb/common/operator/multiply.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/exception/conversion_exception.hpp"
#include "icu-helpers.hpp"

namespace duckdb {

ICUDateFunc::BindData::BindData(const BindData &other)
    : tz_setting(other.tz_setting), cal_setting(other.cal_setting), calendar(other.calendar->Copy()) {
}

ICUDateFunc::BindData::BindData(const string &tz_setting_p, const string &cal_setting_p)
    : tz_setting(tz_setting_p), cal_setting(cal_setting_p) {
	InitCalendar();
}

ICUDateFunc::BindData::BindData(ClientContext &context) {
	Value tz_value;
	if (context.TryGetCurrentSetting("TimeZone", tz_value)) {
		tz_setting = tz_value.ToString();
	}

	Value cal_value;
	if (context.TryGetCurrentSetting("Calendar", cal_value)) {
		cal_setting = cal_value.ToString();
	} else {
		cal_setting = "gregorian";
	}

	InitCalendar();
}

void ICUDateFunc::BindData::InitCalendar() {
	//	Postgres always assumes times are given in the proleptic Gregorian calendar,
	//	which is what our Gregorian calendar implements.
	auto tz = TimeZone::TryCreate(tz_setting);
	calendar = Calendar::TryCreate(cal_setting, tz ? std::move(tz) : TimeZone::TryCreate("UTC"));
	if (!calendar) {
		throw InternalException("Unable to create calendar.");
	}
}

bool ICUDateFunc::BindData::Equals(const FunctionData &other_p) const {
	auto &other = other_p.Cast<const BindData>();
	return calendar->Equals(*other.calendar);
}

unique_ptr<FunctionData> ICUDateFunc::BindData::Copy() const {
	return make_uniq<BindData>(*this);
}

unique_ptr<FunctionData> ICUDateFunc::Bind(BindScalarFunctionInput &input) {
	return make_uniq<BindData>(input.GetClientContext());
}

bool ICUDateFunc::TrySetTimeZone(Calendar *calendar, const string_t &tz_id) {
	string tz_str = tz_id.GetString();
	auto tz = ICUHelpers::TryGetTimeZone(tz_str);
	if (!tz) {
		return false;
	}
	calendar->SetTimeZone(std::move(tz));
	return true;
}

void ICUDateFunc::SetTimeZone(Calendar *calendar, const string_t &tz_id, string *error_message) {
	string tz_str = tz_id.GetString();
	auto tz = ICUHelpers::GetTimeZone(tz_str, error_message);
	if (tz) {
		calendar->SetTimeZone(std::move(tz));
	}
}

timestamp_tz_t ICUDateFunc::GetTimeUnsafe(Calendar *calendar, uint64_t micros) {
	// Extract the new time
	const auto millis = int64_t(calendar->GetTime());
	if (calendar->HasFailed()) {
		throw InternalException("Unable to get calendar time.");
	}
	return timestamp_tz_t(millis * Interval::MICROS_PER_MSEC + int64_t(micros));
}

bool ICUDateFunc::TryGetTime(Calendar *calendar, uint64_t micros, timestamp_tz_t &result) {
	// Extract the new time
	auto millis = int64_t(calendar->GetTime());
	if (calendar->HasFailed()) {
		return false;
	}

	// The time is a double, so it can't overflow (it just loses accuracy), but converting back to µs can.
	if (!TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(millis, Interval::MICROS_PER_MSEC, millis)) {
		return false;
	}
	if (!TryAddOperator::Operation<int64_t, int64_t, int64_t>(millis, int64_t(micros), millis)) {
		return false;
	}

	// Now make sure the value is in range
	result.value = millis;
	date_t out_date = Timestamp::GetDate(timestamp_t(millis));
	int64_t days_micros;
	return TryMultiplyOperator::Operation<int64_t, int64_t, int64_t>(out_date.days, Interval::MICROS_PER_DAY,
	                                                                 days_micros);
}

timestamp_tz_t ICUDateFunc::GetTime(Calendar *calendar, uint64_t micros) {
	timestamp_tz_t result;
	if (!TryGetTime(calendar, micros, result)) {
		throw ConversionException("ICU date overflows timestamp range");
	}
	return result;
}

bool ICUDateFunc::TryGetTimeNS(Calendar *calendar, uint64_t nanos, timestamp_tz_ns_t &result) {
	timestamp_tz_t tstz_micros;
	if (!TryGetTime(calendar, nanos / Interval::NANOS_PER_MICRO, tstz_micros)) {
		return false;
	}

	nanos %= Interval::NANOS_PER_MICRO;
	timestamp_t us(tstz_micros);
	timestamp_ns_t ns;
	if (!Timestamp::TryFromTimestampNanos(us, nanos, ns)) {
		return false;
	}
	result.value = ns.value;
	return true;
}

timestamp_tz_ns_t ICUDateFunc::GetTimeNS(Calendar *calendar, uint64_t nanos) {
	timestamp_tz_ns_t result;
	if (!TryGetTimeNS(calendar, nanos, result)) {
		throw ConversionException("ICU date overflows timestamp_ns range");
	}
	return result;
}

uint64_t ICUDateFunc::SetTime(Calendar *calendar, timestamp_tz_t date) {
	int64_t millis = date.value / Interval::MICROS_PER_MSEC;
	int64_t micros = date.value % Interval::MICROS_PER_MSEC;
	if (micros < 0) {
		--millis;
		micros += Interval::MICROS_PER_MSEC;
	}

	calendar->SetTime(double(millis));
	return uint64_t(micros);
}

uint64_t ICUDateFunc::SetTimeNS(Calendar *calendar, timestamp_tz_ns_t date) {
	int64_t millis = date.value / Interval::NANOS_PER_MSEC;
	int64_t nanos = date.value % Interval::NANOS_PER_MSEC;
	if (nanos < 0) {
		--millis;
		nanos += Interval::NANOS_PER_MSEC;
	}

	calendar->SetTime(double(millis));
	return uint64_t(nanos);
}

int32_t ICUDateFunc::ExtractField(Calendar *calendar, CalendarField field) {
	const auto result = calendar->Get(field);
	// a date the calendar cannot represent is something the query asked for, not a defect
	if (calendar->HasFailed()) {
		throw ConversionException("Unable to extract calendar part");
	}
	return result;
}

int32_t ICUDateFunc::SubtractField(Calendar *calendar, CalendarField field, timestamp_tz_t end_date) {
	const int64_t millis = end_date.value / Interval::MICROS_PER_MSEC;
	const auto sub = calendar->FieldDifference(double(millis), field);
	if (calendar->HasFailed()) {
		throw ConversionException("Unable to subtract calendar part");
	}
	return sub;
}

} // namespace duckdb
