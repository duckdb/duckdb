//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/profiler.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/helper.hpp"

namespace duckdb {

//! Profiler class to measure the elapsed time.
template <typename T>
class BaseProfiler {
public:
	//! Start the timer.
	void Start() {
		finished = false;
		ran = true;
		start = Tick();
	}
	//! End the timer.
	void End() {
		end = Tick();
		finished = true;
	}
	//! Reset the timer.
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
		auto measured_end = finished ? end : Tick();
		return std::chrono::duration_cast<std::chrono::duration<double>>(measured_end - start).count();
	}

private:
	//! Current time point.
	time_point<T> Tick() const {
		return T::now();
	}
	//! Start time point.
	time_point<T> start;
	//! End time point.
	time_point<T> end;
	//! True, if end End() been called.
	bool finished = false;
	//! True, if the timer was ran.
	bool ran = false;
};

using Profiler = BaseProfiler<steady_clock>;

} // namespace duckdb
