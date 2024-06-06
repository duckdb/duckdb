#include "duckdb/transaction/timestamp_manager.hpp"

#include <algorithm>
#include <time.h> /* for clock_gettime */

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wexit-time-destructors"
#endif

namespace duckdb {
transaction_t TimestampManager::timestamp = 0;
mutex TimestampManager::timestamp_lock;
#define PHYSICAL_TIME_MASK        0xffffffffffffff00
#define LOGICAL_COUNTER_MASK      0x00000000000000ff
#define PHYSICAL_TIME_ROUNDUP_BIT 0x0000000000000080
#define PHYSICAL_TIME_ROUNDUP     0x0000000000000100

// Windows monotonic time support
#ifdef _MSC_VER

/**
 * implementation of clock_gettime(CLOCK_MONOTONIC, tv) from unistd.h for Windows
 */

#ifndef WIN32_LEAN_AND_MEAN
	#define WIN32_LEAN_AND_MEAN
#endif
#include <Windows.h>
#undef max
#undef min
#define MS_PER_SEC 1000ULL // MS = milliseconds
#define US_PER_MS 1000ULL  // US = microseconds
#define HNS_PER_US 10ULL   // HNS = hundred-nanoseconds (e.g., 1 hns = 100 ns)
#define NS_PER_US 1000ULL

#define HNS_PER_SEC (MS_PER_SEC * US_PER_MS * HNS_PER_US)
#define NS_PER_HNS (100ULL) // NS = nanoseconds
#define NS_PER_SEC (MS_PER_SEC * US_PER_MS * NS_PER_US)

// https://stackoverflow.com/questions/5404277/porting-clock-gettime-to-windows
void TimestampManager::ClockGetTimeMonotonic(struct timespec *tv)
{
	static LARGE_INTEGER ticksPerSec;
	LARGE_INTEGER ticks;

	if (!ticksPerSec.QuadPart)
	{
		QueryPerformanceFrequency(&ticksPerSec);
		if (!ticksPerSec.QuadPart)
		{
			errno = ENOTSUP;
			fprintf(stderr, "clock_gettime_monotonic: QueryPerformanceFrequency failed\n");
			exit(-1);
		}
	}

	QueryPerformanceCounter(&ticks);

	tv->tv_sec = (long)(ticks.QuadPart / ticksPerSec.QuadPart);
	tv->tv_nsec = (long)(((ticks.QuadPart % ticksPerSec.QuadPart) * NS_PER_SEC) / ticksPerSec.QuadPart);
}
#else

void TimestampManager::ClockGetTimeMonotonic(struct timespec *tv) {
	clock_gettime(CLOCK_MONOTONIC, tv);
}

#endif

transaction_t TimestampManager::GetHLCTimestamp() {
	struct timespec ts;
	lock_guard<mutex> lock(timestamp_lock);
	TimestampManager::ClockGetTimeMonotonic(&ts);
	uint64_t ns = ((uint64_t) ts.tv_sec * BILLION) + (uint64_t) ts.tv_nsec;
	uint64_t pt = ns & PHYSICAL_TIME_MASK;
	uint32_t rb = ns & PHYSICAL_TIME_ROUNDUP_BIT;
	if (rb) {
		pt += PHYSICAL_TIME_ROUNDUP;
	}
	uint64_t new_timestamp;
	uint64_t current_pt = timestamp & PHYSICAL_TIME_MASK;
	uint64_t current_logical_counter = timestamp & LOGICAL_COUNTER_MASK;
	if (current_pt == pt) {
		++current_logical_counter;
		new_timestamp = pt | current_logical_counter;
	} else {
		current_logical_counter = 0;
		new_timestamp = std::max(pt, current_pt) | current_logical_counter;
	}

	timestamp = new_timestamp;
	return new_timestamp;
}

void TimestampManager::SetHLCTimestamp(transaction_t message_ts) {
	struct timespec ts;
	lock_guard<mutex> lock(timestamp_lock);
	TimestampManager::ClockGetTimeMonotonic(&ts);
	uint64_t ns = ((uint64_t) ts.tv_sec * BILLION) + (uint64_t) ts.tv_nsec;
	uint64_t pt = ns & PHYSICAL_TIME_MASK;
	uint32_t rb = ns & PHYSICAL_TIME_ROUNDUP_BIT;
	if (rb) {
		pt += PHYSICAL_TIME_ROUNDUP;
	}

	uint64_t new_timestamp;
	uint64_t current_pt = timestamp & PHYSICAL_TIME_MASK;
	uint64_t message_pt = message_ts & PHYSICAL_TIME_MASK;
	uint64_t message_logical_counter = message_ts & LOGICAL_COUNTER_MASK;
	uint64_t max_pt = std::max({pt, current_pt, message_pt});
	uint64_t current_logical_counter = timestamp & LOGICAL_COUNTER_MASK;
	if ((current_pt == pt) && (pt == message_pt)) {
		current_logical_counter = std::max(current_logical_counter, message_logical_counter) + 1;
		new_timestamp = pt | current_logical_counter;
	} else if (max_pt == current_pt) {
		++current_logical_counter;
	} else if (max_pt == message_pt) {
		current_logical_counter = message_logical_counter + 1;
	} else {
		current_logical_counter = 0;
	}
	new_timestamp = max_pt | current_logical_counter;
	timestamp = new_timestamp;
}
} // namespace duckdb

#ifdef __clang__
#pragma clang diagnostic pop
#endif