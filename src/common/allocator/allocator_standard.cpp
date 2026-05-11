#include "duckdb/common/allocator.hpp"

#ifdef __GLIBC__
#include <malloc.h>
#endif

namespace duckdb {

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
	static atomic<int64_t> LAST_TRIM_TIMESTAMP_MS {0};

	int64_t last_trim_timestamp_ms = LAST_TRIM_TIMESTAMP_MS.load();
	auto current_ts = Timestamp::GetCurrentTimestamp();
	auto current_timestamp_ms = Cast::Operation<timestamp_t, timestamp_ms_t>(current_ts).value;

	if (current_timestamp_ms - last_trim_timestamp_ms < TRIM_INTERVAL_MS) {
		return; // We trimmed less than TRIM_INTERVAL_MS ago
	}
	if (!LAST_TRIM_TIMESTAMP_MS.compare_exchange_strong(last_trim_timestamp_ms, current_timestamp_ms,
	                                                    std::memory_order_acquire, std::memory_order_relaxed)) {
		return; // Another thread has updated LAST_TRIM_TIMESTAMP_MS since we loaded it
	}

	// We successfully updated LAST_TRIM_TIMESTAMP_MS, we can trim
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
