//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/profiler/metric_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/string.hpp"
#include "duckdb/common/winapi.hpp"

namespace duckdb {

struct MetricInfo {
	string name;
	string metric_type; // "double" | "uint64" | "string" | "map"
	string description;
	string unit;
};

//! The most metric indexes a process can have, the profiler's own included
static constexpr idx_t MAX_METRIC_COUNT = 16384;

} // namespace duckdb
