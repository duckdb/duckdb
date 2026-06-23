#include "duckdb/common/allocator.hpp"

#ifndef DUCKDB_ENABLE_JEMALLOC

#include "duckdb/common/assert.hpp"
#include "duckdb/common/atomic.hpp"
#include "duckdb/common/exception.hpp"
#include "duckdb/common/helper.hpp"
#include "duckdb/common/types/interval.hpp"
#include "duckdb/common/types/timestamp.hpp"

#ifdef __GLIBC__
#include <malloc.h>
#endif

#ifdef DUCKDB_DEBUG_ALLOCATION
#include "duckdb/common/mutex.hpp"
#include "duckdb/common/pair.hpp"
#include "duckdb/common/unordered_map.hpp"

#include <execinfo.h>
#endif

namespace duckdb {

namespace {
int64_t GetMonotonicMillis() {
	return Timestamp::GetMonotonicTimestamp().value / Interval::MICROS_PER_MSEC;
}
} // namespace

data_ptr_t Allocator::DefaultAllocate(PrivateAllocatorData *private_data, idx_t size) {
	auto default_allocate_result = malloc(size);
	if (!default_allocate_result) {
		throw std::bad_alloc();
	}
	return data_ptr_cast(default_allocate_result);
}

void Allocator::DefaultFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	free(pointer);
}

data_ptr_t Allocator::DefaultReallocate(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                        idx_t size) {
	return data_ptr_cast(realloc(pointer, size));
}

optional_idx Allocator::DecayDelay() {
	return optional_idx();
}

bool Allocator::SupportsFlush() {
#if defined(__GLIBC__)
	return true;
#else
	return false;
#endif
}

static void MallocTrim(idx_t pad) {
#ifdef __GLIBC__
	static constexpr int64_t TRIM_INTERVAL_MS = 100;
	static atomic<int64_t> LAST_TRIM_TIME_MS {GetMonotonicMillis()};

	int64_t last_trim_time_ms = LAST_TRIM_TIME_MS.load();
	auto current_time_ms = GetMonotonicMillis();

	if (current_time_ms - last_trim_time_ms < TRIM_INTERVAL_MS) {
		return; // We trimmed less than TRIM_INTERVAL_MS ago
	}
	if (!LAST_TRIM_TIME_MS.compare_exchange_strong(last_trim_time_ms, current_time_ms, std::memory_order_acquire,
	                                               std::memory_order_relaxed)) {
		return; // Another thread has updated LAST_TRIM_TIME_MS since we loaded it
	}

	// We successfully updated LAST_TRIM_TIME_MS, we can trim
	malloc_trim(pad);
#endif
}

void Allocator::ThreadFlush(bool allocator_background_threads, idx_t threshold, idx_t thread_count) {
	MallocTrim(thread_count * threshold);
}

void Allocator::ThreadIdle() {
	/* no-op */
}

void Allocator::FlushAll() {
	MallocTrim(0);
}

void Allocator::SetBackgroundThreads(bool enable) {
	/* no-op */
}

} // namespace duckdb

#endif
