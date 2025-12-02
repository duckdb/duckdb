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

namespace duckdb {
using std::thread;

}

#endif

namespace duckdb {

struct ThreadUtil {
	static void SleepMs(idx_t ms);
};

} // namespace duckdb
