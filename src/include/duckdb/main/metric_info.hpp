//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/main/metric_info.hpp
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

} // namespace duckdb
