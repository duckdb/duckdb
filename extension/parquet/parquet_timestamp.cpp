#include "parquet_timestamp.hpp"

#include "duckdb.hpp"
#ifndef DUCKDB_AMALGAMATION
#include "duckdb/common/types/date.hpp"
#include "duckdb/common/types/time.hpp"
#include "duckdb/common/types/timestamp.hpp"
#endif

namespace duckdb {

// surely they are joking
static constexpr int64_t JULIAN_TO_UNIX_EPOCH_DAYS = 2440588LL;
static constexpr int64_t MILLISECONDS_PER_DAY = 86400000LL;
static constexpr int64_t NANOSECONDS_PER_DAY = MILLISECONDS_PER_DAY * 1000LL * 1000LL;

int64_t ImpalaTimestampToNanoseconds(const Int96 &impala_timestamp) {
	int64_t days_since_epoch = impala_timestamp.value[2] - JULIAN_TO_UNIX_EPOCH_DAYS;
	auto nanoseconds = Load<int64_t>((data_ptr_t)impala_timestamp.value);
	return days_since_epoch * NANOSECONDS_PER_DAY + nanoseconds;
}

timestamp_t ImpalaTimestampToTimestamp(const Int96 &raw_ts) {
	auto impala_ns = ImpalaTimestampToNanoseconds(raw_ts);
	return Timestamp::FromEpochNanoSeconds(impala_ns);
}

Int96 TimestampToImpalaTimestamp(timestamp_t &ts) {
	int32_t hour, min, sec, msec;
	Time::Convert(Timestamp::GetTime(ts), hour, min, sec, msec);
	uint64_t ms_since_midnight = hour * 60 * 60 * 1000 + min * 60 * 1000 + sec * 1000 + msec;
	auto days_since_epoch = Date::Epoch(Timestamp::GetDate(ts)) / (24 * 60 * 60);
	// first two uint32 in Int96 are nanoseconds since midnights
	// last uint32 is number of days since year 4713 BC ("Julian date")
	Int96 impala_ts;
	Store<uint64_t>(ms_since_midnight * 1000000, (data_ptr_t)impala_ts.value);
	impala_ts.value[2] = days_since_epoch + JULIAN_TO_UNIX_EPOCH_DAYS;
	return impala_ts;
}

timestamp_t ParquetTimestampMicrosToTimestamp(const int64_t &raw_ts) {
	return Timestamp::FromEpochMicroSeconds(raw_ts);
}
timestamp_t ParquetTimestampMsToTimestamp(const int64_t &raw_ts) {
	return Timestamp::FromEpochMs(raw_ts);
}
timestamp_t ParquetTimestampNsToTimestamp(const int64_t &raw_ts) {
	return Timestamp::FromEpochNanoSeconds(raw_ts);
}

date_t ParquetIntToDate(const int32_t &raw_date) {
	return date_t(raw_date);
}

dtime_t ParquetIntToTime(const int64_t &raw_time) {
	return dtime_t(raw_time);
}

} // namespace duckdb
