//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/thread.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#ifndef DUCKDB_NO_THREADS
#include <thread>
#include "duckdb/common/typedefs.hpp"
#include "duckdb/common/string.hpp"

namespace duckdb {
using std::thread;
using thread_id = std::thread::id;

} // namespace duckdb

#else
using thread_id = uint64_t;
#endif

namespace duckdb {

struct ThreadUtil {
	static void SleepMs(idx_t ms);
	static void SleepMicroSeconds(idx_t micros);
	static thread_id GetThreadId();
	static string GetThreadIdString();
};

} // namespace duckdb
