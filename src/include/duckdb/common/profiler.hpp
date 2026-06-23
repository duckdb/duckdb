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

//! Measures elapsed time with explicit Start/End lifecycle.
//! For simple elapsed-since queries, use TimePoint directly.
class Profiler {
public:
	void Start() {
		finished = false;
		ran = true;
		start = TimePoint::Tick();
	}
	void End() {
		end = TimePoint::Tick();
		finished = true;
	}
	void Reset() {
		finished = false;
		ran = false;
	}

	//! Returns 0 if not started; total elapsed if End() was called; current elapsed otherwise.
	double Elapsed() const {
		if (!ran) {
			return 0;
		}
		int64_t elapsed_nanos;
		if (finished) {
			elapsed_nanos = TimePoint::ElapsedNanosSince(start, end);
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
			return static_cast<idx_t>(TimePoint::ElapsedNanosSince(start, end));
		} else {
			return static_cast<idx_t>(start.ElapsedNanos());
		}
	}

private:
	TimePoint start;
	TimePoint end;
	bool finished = false;
	bool ran = false;
};

} // namespace duckdb
