//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/time_point.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/chrono.hpp"

namespace duckdb {

//! Monotonic clock time point for measuring intervals.
//! Unlike timestamp_t, uses steady_clock unaffected by system clock adjustments.
class TimePoint {
public:
	//! Default constructor for containers; use Tick() for current time.
	TimePoint() : time_point() {
	}

	static TimePoint Tick() {
		return TimePoint(steady_clock::now());
	}

	static int64_t ElapsedNanosSince(const TimePoint &start, const TimePoint &end) {
		return std::chrono::duration_cast<std::chrono::nanoseconds>(end.time_point - start.time_point).count();
	}

	int64_t ElapsedMillis() const {
		auto now = steady_clock::now();
		return std::chrono::duration_cast<std::chrono::milliseconds>(now - time_point).count();
	}

	int64_t ElapsedMicros() const {
		auto now = steady_clock::now();
		return std::chrono::duration_cast<std::chrono::microseconds>(now - time_point).count();
	}

	int64_t ElapsedNanos() const {
		auto now = steady_clock::now();
		return std::chrono::duration_cast<std::chrono::nanoseconds>(now - time_point).count();
	}

private:
	explicit TimePoint(time_point<steady_clock> tp) : time_point(tp) {
	}

	time_point<steady_clock> time_point;
};

} // namespace duckdb
