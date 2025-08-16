//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_timestamp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"

namespace duckdb {

struct Int96 {
	uint32_t value[3];
};

timestamp_t ImpalaTimestampToTimestamp(const Int96 &raw_ts);
timestamp_ns_t ImpalaTimestampToTimestampNS(const Int96 &raw_ts);
Int96 TimestampToImpalaTimestamp(timestamp_t &ts);

timestamp_t ParquetTimestampMicrosToTimestamp(const int64_t &raw_ts);
timestamp_t ParquetTimestampMsToTimestamp(const int64_t &raw_ts);
timestamp_t ParquetTimestampNsToTimestamp(const int64_t &raw_ts);

timestamp_ns_t ParquetTimestampMsToTimestampNs(const int64_t &raw_ms);
timestamp_ns_t ParquetTimestampUsToTimestampNs(const int64_t &raw_us);
timestamp_ns_t ParquetTimestampNsToTimestampNs(const int64_t &raw_ns);

date_t ParquetIntToDate(const int32_t &raw_date);
dtime_t ParquetMsIntToTime(const int32_t &raw_millis);
dtime_t ParquetIntToTime(const int64_t &raw_micros);
dtime_t ParquetNsIntToTime(const int64_t &raw_nanos);

dtime_ns_t ParquetMsIntToTimeNs(const int32_t &raw_millis);
dtime_ns_t ParquetUsIntToTimeNs(const int64_t &raw_micros);
dtime_ns_t ParquetIntToTimeNs(const int64_t &raw_nanos);

dtime_tz_t ParquetIntToTimeMsTZ(const int32_t &raw_millis);
dtime_tz_t ParquetIntToTimeTZ(const int64_t &raw_micros);
dtime_tz_t ParquetIntToTimeNsTZ(const int64_t &raw_nanos);

} // namespace duckdb
