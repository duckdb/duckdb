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

}
