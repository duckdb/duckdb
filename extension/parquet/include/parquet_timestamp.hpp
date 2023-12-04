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
Int96 TimestampToImpalaTimestamp(timestamp_t &ts);
timestamp_t ParquetTimestampMicrosToTimestamp(const int64_t &raw_ts);
timestamp_t ParquetTimestampMsToTimestamp(const int64_t &raw_ts);
timestamp_t ParquetTimestampNsToTimestamp(const int64_t &raw_ts);
date_t ParquetIntToDate(const int32_t &raw_date);
dtime_t ParquetIntToTimeMs(const int32_t &raw_time);
dtime_t ParquetIntToTime(const int64_t &raw_time);
dtime_t ParquetIntToTimeNs(const int64_t &raw_time);
dtime_tz_t ParquetIntToTimeTZ(const int64_t &raw_time);

} // namespace duckdb
