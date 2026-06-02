#include "duckdb/common/thread.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/original/std/sstream.hpp"

namespace duckdb {

#ifndef DUCKDB_NO_THREADS
void ThreadUtil::SleepMs(idx_t sleep_ms) {
	static constexpr idx_t SLEEP_INTERVAL_MS = 1;
	for (idx_t remaining = sleep_ms; remaining > 0; remaining -= std::min(remaining, SLEEP_INTERVAL_MS)) {
		std::this_thread::sleep_for(std::chrono::milliseconds(std::min(remaining, SLEEP_INTERVAL_MS)));
	}
}

void ThreadUtil::SleepMicroSeconds(idx_t micros) {
	std::this_thread::sleep_for(std::chrono::microseconds(micros));
}

thread_id ThreadUtil::GetThreadId() {
	return std::this_thread::get_id();
}

string ThreadUtil::GetThreadIdString() {
	std::ostringstream ss;
	ss << std::this_thread::get_id();
	return ss.str();
}

#else

void ThreadUtil::SleepMs(idx_t sleep_ms) {
	throw InvalidInputException("ThreadUtil::SleepMs requires DuckDB to be compiled with thread support");
}

void ThreadUtil::SleepMicroSeconds(idx_t micros) {
	throw InvalidInputException("ThreadUtil::SleepMicroSeconds requires DuckDB to be compiled with thread support");
}

thread_id ThreadUtil::GetThreadId() {
	return 0;
}

string ThreadUtil::GetThreadIdString() {
	return "0";
}

#endif
} // namespace duckdb
