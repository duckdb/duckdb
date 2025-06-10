#pragma once

#include "duckdb/common/mutex.hpp"
#include "duckdb/common/typedefs.hpp"

namespace duckdb {

//! The Timestamp Manager is responsible for creating and managing
//! timestamps
class TimestampManager {
public:
	static transaction_t GetHLCTimestamp();
	static void SetHLCTimestamp(transaction_t ts);

private:
	static const uint64_t BILLION = 1000000000L;
	static transaction_t timestamp;
	static mutex timestamp_lock;
	static void ClockGetTimeMonotonic(struct timespec *tv);

};

} // namespace duckdb