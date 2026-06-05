#include "duckdb/common/thread.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/original/std/sstream.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

#ifndef DUCKDB_NO_THREADS
void ThreadUtil::SleepMs(idx_t sleep_ms, optional_ptr<ClientContext> context) {
	auto target_time = Timestamp::GetCurrentTimestamp();
	target_time.value += sleep_ms * Interval::MICROS_PER_MSEC;
	static constexpr idx_t DEFAULT_SLEEP_INTERVAL_MS = 100;

	auto sleep_interval = MinValue(DEFAULT_SLEEP_INTERVAL_MS, sleep_ms);
	while (Timestamp::GetCurrentTimestamp() < target_time) {
		// check interrupt flag
		if (context && context->IsInterrupted()) {
			throw InterruptException();
		}
		std::this_thread::sleep_for(milliseconds(sleep_interval));
	}
}

void ThreadUtil::SleepMicroSeconds(idx_t micros) {
	std::this_thread::sleep_for(microseconds(micros));
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
