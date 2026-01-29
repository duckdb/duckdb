#include "duckdb/common/thread.hpp"
#include "duckdb/common/chrono.hpp"

namespace duckdb {

void ThreadUtil::SleepMs(idx_t sleep_ms) {
#ifndef DUCKDB_NO_THREADS
	std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
#else
	throw InvalidInputException("ThreadUtil::SleepMs requires DuckDB to be compiled with thread support");
#endif
}

void ThreadUtil::SleepMicroSeconds(idx_t sleep_ms) {
#ifndef DUCKDB_NO_THREADS
	std::this_thread::sleep_for(std::chrono::microseconds(sleep_ms));
#else
	throw InvalidInputException("ThreadUtil::SleepMicroSeconds requires DuckDB to be compiled with thread support");
#endif
}

} // namespace duckdb
