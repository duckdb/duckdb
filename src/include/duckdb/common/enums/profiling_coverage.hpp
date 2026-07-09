//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/enums/profiling_coverage.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <cstdint>

namespace duckdb {

enum class ProfilingCoverage : uint8_t { SELECT = 0, ALL = 1 };

} // namespace duckdb
