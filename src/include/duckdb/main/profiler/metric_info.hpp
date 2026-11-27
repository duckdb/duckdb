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

//! How the profiler updates a metric with an index: a "uint64" counts, a "double" times
enum class MetricSlotKind : uint8_t { NONE, COUNTER, TIMING };

} // namespace duckdb
