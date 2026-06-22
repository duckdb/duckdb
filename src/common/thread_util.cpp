#include "duckdb/common/thread.hpp"
#include "duckdb/common/chrono.hpp"
#include "duckdb/original/std/sstream.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/checked_integer.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/types/timestamp.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/main/client_context.hpp"

namespace duckdb {

#ifndef DUCKDB_NO_THREADS
void ThreadUtil::SleepMs(idx_t sleep_ms, optional_ptr<ClientContext> context) {
	using checked_int64_t = CheckedInteger<int64_t, InvalidInputException>;
	auto target_time = Timestamp::GetCurrentTimestamp();
	checked_int64_t sleep_duration(sleep_ms);
	auto sleep_micros = sleep_duration * Interval::MICROS_PER_MSEC;
	checked_int64_t target_value(target_time.value);
	target_time.value = (target_value + sleep_micros).GetValue();
	static constexpr idx_t DEFAULT_SLEEP_INTERVAL_MS = 100;

	while (true) {
		auto current_time = Timestamp::GetCurrentTimestamp();
		if (context && context->IsInterrupted()) {
			throw InterruptException();
		}
		if (current_time >= target_time) {
			break;
		}
		auto remaining_ms = static_cast<idx_t>(target_time.value - current_time.value) / Interval::MICROS_PER_MSEC;
		std::this_thread::sleep_for(milliseconds(MinValue(remaining_ms, DEFAULT_SLEEP_INTERVAL_MS)));
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

void ThreadUtil::SleepMs(idx_t sleep_ms, optional_ptr<ClientContext>) {
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
