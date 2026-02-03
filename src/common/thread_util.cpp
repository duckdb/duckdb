#include "duckdb/common/thread.hpp"
#include "duckdb/common/chrono.hpp"

namespace duckdb {

#ifndef DUCKDB_NO_THREADS
void ThreadUtil::SleepMs(idx_t sleep_ms) {
	std::this_thread::sleep_for(std::chrono::milliseconds(sleep_ms));
}

thread_id ThreadUtil::GetThreadId() {
	return std::this_thread::get_id();
}
#else

void ThreadUtil::SleepMs(idx_t sleep_ms) {
	throw InvalidInputException("ThreadUtil::SleepMs requires DuckDB to be compiled with thread support");
}

thread_id ThreadUtil::GetThreadId() {
	return 0;
}
#endif
} // namespace duckdb
