//===----------------------------------------------------------------------===//
//                         DuckDB
//
// calendar_reference.cpp
//
// Temporary: verifies the built-in calendar implementation against ICU.
// This file (and the icu_calendar_verify function) is removed once ICU is gone.
//
//===----------------------------------------------------------------------===//

#include "calendar.hpp"
#include "duckdb/function/table_function.hpp"
#include "duckdb/main/extension/extension_loader.hpp"
#include "grego.hpp"
#include "unicode/calendar.h"
#include "unicode/timezone.h"
#include "unicode/ucal.h"

namespace duckdb {
namespace datetime {

namespace {

struct Mismatch {
	string zone;
	double millis;
	string operation;
	int64_t expected;
	int64_t actual;
};

struct VerifyData : public GlobalTableFunctionState {
	vector<Mismatch> mismatches;
	idx_t offset = 0;
};

//! All of the fields that both implementations compute
const CalendarField ALL_FIELDS[] = {CAL_ERA,
                                    CAL_YEAR,
                                    CAL_MONTH,
                                    CAL_WEEK_OF_YEAR,
                                    CAL_WEEK_OF_MONTH,
                                    CAL_DATE,
                                    CAL_DAY_OF_YEAR,
                                    CAL_DAY_OF_WEEK,
                                    CAL_DAY_OF_WEEK_IN_MONTH,
                                    CAL_AM_PM,
                                    CAL_HOUR,
                                    CAL_HOUR_OF_DAY,
                                    CAL_MINUTE,
                                    CAL_SECOND,
                                    CAL_MILLISECOND,
                                    CAL_ZONE_OFFSET,
                                    CAL_DST_OFFSET,
                                    CAL_YEAR_WOY,
                                    CAL_DOW_LOCAL,
                                    CAL_EXTENDED_YEAR,
                                    CAL_JULIAN_DAY,
                                    CAL_MILLISECONDS_IN_DAY};

const char *FieldName(CalendarField field) {
	static const char *NAMES[] = {"era",
	                              "year",
	                              "month",
	                              "week_of_year",
	                              "week_of_month",
	                              "date",
	                              "day_of_year",
	                              "day_of_week",
	                              "day_of_week_in_month",
	                              "am_pm",
	                              "hour",
	                              "hour_of_day",
	                              "minute",
	                              "second",
	                              "millisecond",
	                              "zone_offset",
	                              "dst_offset",
	                              "year_woy",
	                              "dow_local",
	                              "extended_year",
	                              "julian_day",
	                              "milliseconds_in_day",
	                              "is_leap_month",
	                              "ordinal_month"};
	return NAMES[field];
}

//! Runs the same operations on both implementations and records every difference
class Comparator {
public:
	Comparator(string zone_p, icu::Calendar &expected_p, Calendar &actual_p, vector<Mismatch> &mismatches_p)
	    : zone(std::move(zone_p)), expected(expected_p), actual(actual_p), mismatches(mismatches_p), millis(0) {
	}

	bool Failed() const {
		return mismatches.size() > 100;
	}

	void Report(const string &operation, int64_t expected_value, int64_t actual_value) {
		if (expected_value != actual_value) {
			mismatches.push_back({zone, millis, operation, expected_value, actual_value});
		}
	}

	void SetTime(double time) {
		// an operation that failed leaves the state of the calendar undefined, which the
		// extension never observes because it reports the failure straight away
		desynchronized = false;
		millis = time;
		UErrorCode status = U_ZERO_ERROR;
		expected.setTime(time, status);
		actual.SetTime(time);
	}

	//! Reads a field from both, reporting a difference, and returns the reference value
	int32_t Get(const string &tag, CalendarField field) {
		if (desynchronized) {
			return 0;
		}
		UErrorCode status = U_ZERO_ERROR;
		const auto expected_value = expected.get(UCalendarDateFields(field), status);
		const auto actual_value = actual.Get(field);
		if (U_FAILURE(status) || actual.HasFailed()) {
			Report(tag + " get " + FieldName(field) + " failure", U_FAILURE(status), actual.HasFailed());
			desynchronized = true;
			return expected_value;
		}
		Report(tag + " get " + FieldName(field), expected_value, actual_value);
		return expected_value;
	}

	void GetAllFields(const string &tag) {
		for (const auto field : ALL_FIELDS) {
			Get(tag, field);
		}
	}

	void Set(CalendarField field, int32_t value) {
		expected.set(UCalendarDateFields(field), value);
		actual.Set(field, value);
	}

	void Add(const string &tag, CalendarField field, int32_t amount) {
		if (desynchronized) {
			return;
		}
		UErrorCode status = U_ZERO_ERROR;
		expected.add(UCalendarDateFields(field), amount, status);
		actual.Add(field, amount);
		if (U_FAILURE(status) || actual.HasFailed()) {
			Report(tag + " add " + FieldName(field) + " failure", U_FAILURE(status), actual.HasFailed());
			desynchronized = true;
		}
	}

	void GetTime(const string &tag) {
		if (desynchronized) {
			return;
		}
		UErrorCode status = U_ZERO_ERROR;
		const auto expected_value = double(expected.getTime(status));
		const auto actual_value = actual.GetTime();
		if (U_FAILURE(status) || actual.HasFailed()) {
			Report(tag + " time failure", U_FAILURE(status), actual.HasFailed());
			desynchronized = true;
			return;
		}
		Report(tag + " time", int64_t(expected_value), int64_t(actual_value));
	}

	void GetActualMaximum(const string &tag, CalendarField field) {
		UErrorCode status = U_ZERO_ERROR;
		const auto expected_value = expected.getActualMaximum(UCalendarDateFields(field), status);
		const auto actual_value = actual.GetActualMaximum(field);
		Report(tag + " actual maximum " + FieldName(field), expected_value, actual_value);
	}

	void FieldDifference(const string &tag, double target, CalendarField field) {
		if (desynchronized) {
			return;
		}
		UErrorCode status = U_ZERO_ERROR;
		const auto expected_value = expected.fieldDifference(target, UCalendarDateFields(field), status);
		const auto actual_value = actual.FieldDifference(target, field);
		if (U_FAILURE(status) || actual.HasFailed()) {
			Report(tag + " difference " + FieldName(field) + " failure", U_FAILURE(status), actual.HasFailed());
			desynchronized = true;
			return;
		}
		Report(tag + " difference " + FieldName(field), expected_value, actual_value);
	}

	//! Switches the zone without resetting the time, which the extension does per row
	void SetZone(const string &zone_name) {
		expected.adoptTimeZone(
		    icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(zone_name))));
		actual.SetTimeZone(TimeZone::TryCreate(zone_name));
	}

	void SetWeek(int32_t first_day_of_week, int32_t minimal_days) {
		expected.setFirstDayOfWeek(UCalendarDaysOfWeek(first_day_of_week));
		expected.setMinimalDaysInFirstWeek(uint8_t(minimal_days));
		actual.SetFirstDayOfWeek(first_day_of_week);
		actual.SetMinimalDaysInFirstWeek(minimal_days);
	}

private:
	string zone;
	icu::Calendar &expected;
	Calendar &actual;
	vector<Mismatch> &mismatches;
	double millis;
	//! Whether an operation failed, after which the state of the calendars is undefined
	bool desynchronized = false;
};

//! Reproduces how the extension truncates a time to a part boundary
void VerifyTruncation(Comparator &comparator, double instant) {
	// the sub-day parts
	for (const auto field : {CAL_MILLISECOND, CAL_SECOND, CAL_MINUTE, CAL_HOUR_OF_DAY}) {
		comparator.SetTime(instant);
		for (const auto cleared : {CAL_MILLISECOND, CAL_SECOND, CAL_MINUTE, CAL_HOUR_OF_DAY}) {
			comparator.Set(cleared, 0);
			if (cleared == field) {
				break;
			}
		}
		comparator.GetTime("truncate to " + string(FieldName(field)));
	}

	// the ISO week, which uses a different week definition
	comparator.SetTime(instant);
	comparator.Set(CAL_MILLISECOND, 0);
	comparator.Set(CAL_SECOND, 0);
	comparator.Set(CAL_MINUTE, 0);
	comparator.Set(CAL_HOUR_OF_DAY, 0);
	comparator.SetWeek(CAL_MONDAY, 4);
	comparator.Set(CAL_DAY_OF_WEEK, CAL_MONDAY);
	comparator.GetTime("truncate to week");
	comparator.SetWeek(CAL_SUNDAY, 1);

	// the month, quarter, year, decade, century, millennium
	comparator.SetTime(instant);
	comparator.Set(CAL_MILLISECOND, 0);
	comparator.Set(CAL_SECOND, 0);
	comparator.Set(CAL_MINUTE, 0);
	comparator.Set(CAL_HOUR_OF_DAY, 0);
	comparator.Set(CAL_DATE, 1);
	comparator.GetTime("truncate to month");
	const auto month = comparator.Get("truncate", CAL_MONTH);
	comparator.Set(CAL_MONTH, (month / 3) * 3);
	comparator.GetTime("truncate to quarter");
	comparator.Set(CAL_MONTH, 0);
	comparator.GetTime("truncate to year");
	const auto year = comparator.Get("truncate", CAL_YEAR);
	comparator.Set(CAL_YEAR, (year / 10) * 10);
	comparator.GetTime("truncate to decade");
	comparator.Set(CAL_YEAR, (year / 100) * 100);
	comparator.GetTime("truncate to century");
	comparator.Set(CAL_YEAR, (year / 1000) * 1000);
	comparator.GetTime("truncate to millennium");

	// the ISO year, which starts at the first week
	comparator.SetTime(instant);
	comparator.Set(CAL_MILLISECOND, 0);
	comparator.Set(CAL_SECOND, 0);
	comparator.Set(CAL_MINUTE, 0);
	comparator.Set(CAL_HOUR_OF_DAY, 0);
	comparator.SetWeek(CAL_MONDAY, 4);
	comparator.Set(CAL_DAY_OF_WEEK, CAL_MONDAY);
	comparator.Set(CAL_WEEK_OF_YEAR, 1);
	comparator.GetTime("truncate to iso year");
	comparator.SetWeek(CAL_SUNDAY, 1);

	// the era, which is how the extension truncates the whole range
	comparator.SetTime(instant);
	const auto era = comparator.Get("truncate", CAL_ERA);
	comparator.Set(CAL_MILLISECOND, 0);
	comparator.Set(CAL_SECOND, 0);
	comparator.Set(CAL_MINUTE, 0);
	comparator.Set(CAL_HOUR_OF_DAY, 0);
	comparator.Set(CAL_DATE, 1);
	comparator.Set(CAL_MONTH, 0);
	comparator.Set(CAL_YEAR, 0);
	comparator.Set(CAL_ERA, era);
	comparator.GetTime("truncate to era");
}

//! Reproduces how the extension builds a time out of its parts
void VerifyConstruction(Comparator &comparator, double instant) {
	comparator.SetTime(instant);
	const auto year = comparator.Get("construct", CAL_EXTENDED_YEAR);
	const auto month = comparator.Get("construct", CAL_MONTH);
	const auto date = comparator.Get("construct", CAL_DATE);

	// the parts are deliberately allowed to fall outside their range, which the extension
	// relies on for make_timestamptz
	for (const auto day_delta : {0, 1, -1, 40, -40}) {
		for (const auto hour : {0, 2, 3, 23, 25, -1}) {
			comparator.Set(CAL_EXTENDED_YEAR, year);
			comparator.Set(CAL_MONTH, month);
			comparator.Set(CAL_DATE, date + day_delta);
			comparator.Set(CAL_HOUR_OF_DAY, hour);
			comparator.Set(CAL_MINUTE, 30);
			comparator.Set(CAL_SECOND, 15);
			comparator.Set(CAL_MILLISECOND, 500);
			comparator.GetTime("construct");
		}
	}

	// with an explicit offset, which is how strptime handles a parsed time zone
	comparator.Set(CAL_EXTENDED_YEAR, year);
	comparator.Set(CAL_MONTH, month);
	comparator.Set(CAL_DATE, date);
	comparator.Set(CAL_HOUR_OF_DAY, 12);
	comparator.Set(CAL_MINUTE, 0);
	comparator.Set(CAL_SECOND, 0);
	comparator.Set(CAL_MILLISECOND, 0);
	comparator.Set(CAL_ZONE_OFFSET, -5 * 60 * 60 * 1000);
	comparator.Set(CAL_DST_OFFSET, 0);
	comparator.GetTime("construct with offset");
}

//! Reproduces how the extension adds intervals and subtracts timestamps
void VerifyArithmetic(Comparator &comparator, double instant) {
	for (const auto field : {CAL_MONTH, CAL_DATE, CAL_HOUR, CAL_MINUTE, CAL_SECOND, CAL_MILLISECOND, CAL_ERA, CAL_YEAR,
	                         CAL_WEEK_OF_YEAR, CAL_DAY_OF_YEAR}) {
		for (const auto amount : {1, -1, 2, -2, 13, -13, 400, -400}) {
			comparator.SetTime(instant);
			comparator.Add("add", field, amount);
			comparator.GetTime("add");
			comparator.Get("add", CAL_HOUR_OF_DAY);
			comparator.Get("add", CAL_DATE);
		}
	}

	// the difference between two instants, as used by age() and date_diff()
	for (const auto field : {CAL_YEAR, CAL_MONTH, CAL_DATE, CAL_WEEK_OF_YEAR}) {
		for (const auto delta : {1.0, -1.0, 1e3, -1e3, 1e7, -1e7, 1e10, -1e10, 1e12, -1e12}) {
			comparator.SetTime(instant);
			const string tag = "difference " + string(FieldName(field)) + " by " + to_string(int64_t(delta));
			comparator.FieldDifference(tag, instant + delta, field);
			comparator.GetTime(tag);
		}
	}
}

//! Reproduces how the extension reuses a calendar across rows with different time zones.
//! The fields are never read in between, so they keep whatever the previous row left behind.
void VerifyZoneSwitching(Comparator &comparator, double instant) {
	static const char *ZONES[] = {"Europe/Rome", "America/Los_Angeles", "Asia/Kathmandu", "UTC"};
	static const int32_t YEARS[] = {44, -43, 1, 0, -1, 2024, -1000};
	comparator.SetTime(instant);
	for (const auto &zone_name : ZONES) {
		for (const auto year : YEARS) {
			comparator.SetZone(zone_name);
			comparator.Set(CAL_YEAR, year);
			comparator.Set(CAL_MONTH, 2);
			comparator.Set(CAL_DATE, 15);
			comparator.Set(CAL_HOUR_OF_DAY, 12);
			comparator.Set(CAL_MINUTE, 45);
			comparator.Set(CAL_SECOND, 42);
			comparator.Set(CAL_MILLISECOND, 0);
			comparator.GetTime("switch zone");
		}
	}
	// the same, but reading the fields back after every row
	comparator.SetTime(instant);
	for (const auto &zone_name : ZONES) {
		for (const auto year : YEARS) {
			comparator.SetZone(zone_name);
			comparator.Set(CAL_YEAR, year);
			comparator.Set(CAL_MONTH, 2);
			comparator.Set(CAL_DATE, 15);
			comparator.GetTime("switch zone and read");
			comparator.Get("switch zone and read", CAL_ERA);
			comparator.Get("switch zone and read", CAL_YEAR);
			comparator.Get("switch zone and read", CAL_EXTENDED_YEAR);
			comparator.Get("switch zone and read", CAL_ZONE_OFFSET);
		}
	}
}

void Verify(const string &type, const string &zone_name, vector<Mismatch> &mismatches, const vector<double> &instants) {
	auto tz = TimeZone::TryCreate(zone_name);
	auto actual = Calendar::TryCreate(type, std::move(tz));
	if (!actual) {
		mismatches.push_back({zone_name, 0, "create " + type + " calendar", 0, 1});
		return;
	}

	string locale_id("@calendar=");
	locale_id += type;
	UErrorCode status = U_ZERO_ERROR;
	duckdb::unique_ptr<icu::Calendar> expected(icu::Calendar::createInstance(
	    icu::TimeZone::createTimeZone(icu::UnicodeString::fromUTF8(icu::StringPiece(zone_name))),
	    icu::Locale(locale_id.c_str()), status));
	if (!expected) {
		mismatches.push_back({zone_name, 0, "create " + type + " calendar", 1, 0});
		return;
	}
	// the extension makes the calendar proleptic Gregorian, which is what Postgres assumes.
	// This fails for the calendars that are not Gregorian, which the extension ignores.
	ucal_setGregorianChange(reinterpret_cast<UCalendar *>(expected.get()), U_DATE_MIN, &status);

	Comparator comparator(type + " " + zone_name, *expected, *actual, mismatches);
	for (const auto instant : instants) {
		comparator.SetTime(instant);
		comparator.GetAllFields("fields");
		comparator.GetActualMaximum("fields", CAL_DATE);
		VerifyTruncation(comparator, instant);
		VerifyConstruction(comparator, instant);
		VerifyArithmetic(comparator, instant);
		VerifyZoneSwitching(comparator, instant);
		if (comparator.Failed()) {
			return;
		}
	}
}

//! The instants at which a zone is compared: a spread over the whole range together with the
//! hours around every transition, where the two implementations are most likely to differ
vector<double> GetInstants(const string &zone_name, idx_t transition_stride) {
	vector<double> instants;
	for (const auto year : {-4000, -1, 1, 1000, 1582, 1900, 1970, 2024, 2038, 10000, 100000}) {
		for (const auto day : {0, 45, 180, 300}) {
			instants.push_back(double(Grego::FieldsToDay(year, 0, 1) + day) * double(MILLIS_PER_DAY) +
			                   13 * double(MILLIS_PER_HOUR) + 4004);
		}
	}
	const auto transitions = TimeZone::GetTransitions(zone_name);
	for (idx_t i = 0; i < transitions.size(); i += transition_stride) {
		const auto transition = double(transitions[i]) * double(MILLIS_PER_SECOND);
		for (const auto hours : {-25, -2, -1, 0, 1, 2, 25}) {
			instants.push_back(transition + hours * double(MILLIS_PER_HOUR));
		}
	}
	return instants;
}

unique_ptr<FunctionData> VerifyBind(ClientContext &context, TableFunctionBindInput &input,
                                    vector<LogicalType> &return_types, vector<string> &names) {
	names = {"zone", "millis", "operation", "expected", "actual"};
	return_types = {LogicalType::VARCHAR, LogicalType::DOUBLE, LogicalType::VARCHAR, LogicalType::BIGINT,
	                LogicalType::BIGINT};
	return nullptr;
}

unique_ptr<GlobalTableFunctionState> VerifyInit(ClientContext &context, TableFunctionInitInput &input) {
	auto result = make_uniq<VerifyData>();
	// a handful of zones cover the interesting rules densely, the rest are sampled
	static const char *DENSE_ZONES[] = {"UTC",
	                                    "America/New_York",
	                                    "Europe/Amsterdam",
	                                    "Australia/Lord_Howe",
	                                    "Pacific/Apia",
	                                    "America/Sao_Paulo",
	                                    "Asia/Tehran",
	                                    "Africa/Casablanca",
	                                    "Antarctica/Troll",
	                                    "America/St_Johns",
	                                    "Pacific/Kiritimati",
	                                    "Asia/Kathmandu",
	                                    "Europe/Dublin",
	                                    "GMT+5:30"};
	// the calendar systems that are no longer provided by ICU
	static const char *NATIVE_TYPES[] = {
	    "gregorian", "buddhist",      "roc",          "iso8601",          "coptic", "ethiopic",           "persian",
	    "indian",    "islamic-civil", "islamic-tbla", "islamic-umalqura", "hebrew", "ethiopic-amete-alem"};
	for (const auto &type : NATIVE_TYPES) {
		for (const auto &zone_name : DENSE_ZONES) {
			Verify(type, zone_name, result->mismatches, GetInstants(zone_name, 1));
			if (result->mismatches.size() > 100) {
				return std::move(result);
			}
		}
	}
	for (const auto &zone_name : TimeZone::GetAvailableIds()) {
		Verify("gregorian", zone_name, result->mismatches, GetInstants(zone_name, 32));
		if (result->mismatches.size() > 100) {
			break;
		}
	}
	return std::move(result);
}

void VerifyFunction(ClientContext &context, TableFunctionInput &data_p, DataChunk &output) {
	auto &data = data_p.global_state->Cast<VerifyData>();
	const auto count = MinValue<idx_t>(STANDARD_VECTOR_SIZE, data.mismatches.size() - data.offset);
	for (idx_t i = 0; i < count; i++) {
		const auto &mismatch = data.mismatches[data.offset + i];
		output.SetValue(0, i, Value(mismatch.zone));
		output.SetValue(1, i, Value::DOUBLE(mismatch.millis));
		output.SetValue(2, i, Value(mismatch.operation));
		output.SetValue(3, i, Value::BIGINT(mismatch.expected));
		output.SetValue(4, i, Value::BIGINT(mismatch.actual));
	}
	data.offset += count;
	output.SetCardinality(count);
}

} // namespace

void RegisterCalendarVerifyFunction(ExtensionLoader &loader) {
	TableFunction verify("icu_calendar_verify", {}, VerifyFunction, VerifyBind, VerifyInit);
	loader.RegisterFunction(verify);
}

} // namespace datetime
} // namespace duckdb
