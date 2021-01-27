//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parquet_timestamp.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/common.hpp"

namespace duckdb {

struct Int96 {
	uint32_t value[3];
};

int64_t impala_timestamp_to_nanoseconds(const Int96 &impala_timestamp);
timestamp_t impala_timestamp_to_timestamp_t(const Int96 &raw_ts);
Int96 timestamp_t_to_impala_timestamp(timestamp_t &ts);
timestamp_t parquet_timestamp_micros_to_timestamp(const int64_t &raw_ts);
timestamp_t parquet_timestamp_ms_to_timestamp(const int64_t &raw_ts);
date_t parquet_int_to_date(const int32_t &raw_date);

} // namespace duckdb
