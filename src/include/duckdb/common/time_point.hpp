//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/time_point.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/chrono.hpp"
#include "duckdb/common/common.hpp"

namespace duckdb {

//! Monotonic clock time point, which is used to measure intervals.
class DUCKDB_API TimePoint {
public:
	TimePoint();

	static TimePoint Tick();

	// Get current monotonic clock time in milliseconds.
	static int64_t GetTickMs();
	// Get two timepoints difference in different units
	static double ElapsedSeconds(const TimePoint &start, const TimePoint &end);
	static int64_t ElapsedMillis(const TimePoint &start, const TimePoint &end);
	static int64_t ElapsedMicros(const TimePoint &start, const TimePoint &end);
	static int64_t ElapsedNanos(const TimePoint &start, const TimePoint &end);

	// Get the elapsed time since the start timepoint in different units
	double ElapsedSeconds() const;
	int64_t ElapsedMillis() const;
	int64_t ElapsedMicros() const;
	int64_t ElapsedNanos() const;

private:
	explicit TimePoint(time_point<steady_clock> value_p);

	time_point<steady_clock> value;
};

} // namespace duckdb
