//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/chrono.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include <chrono>

namespace duckdb {
using std::chrono::duration;
using std::chrono::duration_cast;
using std::chrono::high_resolution_clock;
using std::chrono::milliseconds;
using std::chrono::system_clock;
using std::chrono::time_point;

// The portable way to do this (steady_clock) is not performant on all platforms.
// If we fail to read a timestamp (for whatever reasion) this returns zero.
int64_t GetCoarseMonotonicMillisecondTimestamp();

} // namespace duckdb
