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

	static int64_t ElapsedNanosSince(const TimePoint &start, const TimePoint &end);

	int64_t ElapsedMillis() const;

	int64_t ElapsedMicros() const;

	int64_t ElapsedNanos() const;

private:
	explicit TimePoint(time_point<steady_clock> value_p);

	time_point<steady_clock> value;
};

} // namespace duckdb
