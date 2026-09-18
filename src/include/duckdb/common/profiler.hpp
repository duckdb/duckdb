//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/profiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/time_point.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

//! Profiler class to measure the elapsed time.
class Profiler {
public:
	//! Start the timer.
	void Start() {
		finished = false;
		ran = true;
		start = TimePoint::Tick();
	}
	//! End the timer.
	void End() {
		end = TimePoint::Tick();
		finished = true;
	}
	void Reset() {
		finished = false;
		ran = false;
	}

	//! Returns the elapsed time in seconds.
	//! If ran is false, it returns 0.
	//! If End() has been called, it returns the total elapsed time,
	//! otherwise, returns how far along the timer is right now.
	double Elapsed() const {
		if (!ran) {
			return 0;
		}
		int64_t elapsed_nanos = 0;
		if (finished) {
			elapsed_nanos = TimePoint::ElapsedNanos(start, end);
		} else {
			elapsed_nanos = start.ElapsedNanos();
		}
		return static_cast<double>(elapsed_nanos) / 1e9;
	}

	idx_t ElapsedNanos() const {
		if (!ran) {
			return 0;
		}
		if (finished) {
			return static_cast<idx_t>(TimePoint::ElapsedNanos(start, end));
		}
		return static_cast<idx_t>(start.ElapsedNanos());
	}

private:
	TimePoint start;
	TimePoint end;
	bool finished = false;
	bool ran = false;
};

} // namespace duckdb
