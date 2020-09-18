#include "parquet_timestamp.hpp"
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"

namespace duckdb {

// surely they are joking
static constexpr int64_t kJulianToUnixEpochDays = 2440588LL;
static constexpr int64_t kMillisecondsInADay = 86400000LL;
static constexpr int64_t kNanosecondsInADay = kMillisecondsInADay * 1000LL * 1000LL;

int64_t impala_timestamp_to_nanoseconds(const Int96 &impala_timestamp) {
	int64_t days_since_epoch = impala_timestamp.value[2] - kJulianToUnixEpochDays;
	auto nanoseconds = Load<int64_t>((data_ptr_t)impala_timestamp.value);
	return days_since_epoch * kNanosecondsInADay + nanoseconds;
}

timestamp_t impala_timestamp_to_timestamp_t(const Int96 &raw_ts) {
	auto impala_ns = impala_timestamp_to_nanoseconds(raw_ts);
	auto ms = impala_ns / 1000000; // nanoseconds
	auto ms_per_day = (int64_t)60 * 60 * 24 * 1000;
	date_t date = Date::EpochToDate(ms / 1000);
	dtime_t time = (dtime_t)(ms % ms_per_day);
	return Timestamp::FromDatetime(date, time);
}

Int96 timestamp_t_to_impala_timestamp(timestamp_t &ts) {
	int32_t hour, min, sec, msec;
	Time::Convert(Timestamp::GetTime(ts), hour, min, sec, msec);
	uint64_t ms_since_midnight = hour * 60 * 60 * 1000 + min * 60 * 1000 + sec * 1000 + msec;
	auto days_since_epoch = Date::Epoch(Timestamp::GetDate(ts)) / (24 * 60 * 60);
	// first two uint32 in Int96 are nanoseconds since midnights
	// last uint32 is number of days since year 4713 BC ("Julian date")
	Int96 impala_ts;
	Store<uint64_t>(ms_since_midnight * 1000000, (data_ptr_t)impala_ts.value);
	impala_ts.value[2] = days_since_epoch + kJulianToUnixEpochDays;
	return impala_ts;
}

}