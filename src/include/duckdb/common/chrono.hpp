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
using std::chrono::nanoseconds;
using std::chrono::steady_clock;
using std::chrono::system_clock;
using std::chrono::time_point;
} // namespace duckdb
