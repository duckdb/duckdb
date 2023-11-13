//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/mutex.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifdef __MVS__
#include <time.h>
#endif
#include <mutex>

namespace duckdb {
using std::lock_guard;
using std::mutex;
using std::unique_lock;
} // namespace duckdb
