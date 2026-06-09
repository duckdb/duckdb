#include "duckdb/common/allocator.hpp"
#include "duckdb/common/numeric_utils.hpp"

#include <thread>
#include <cstdint>

#if defined(DUCKDB_ENABLE_JEMALLOC)
#if !defined(WIN32) && INTPTR_MAX == INT64_MAX

#include "jemalloc/jemalloc.h"
#include "duckdb/malloc_ncpus.h"

#else
#error "jemalloc support is only available on 64-bit Linux with DUCKDB_ENABLE_JEMALLOC enabled"
#endif

extern "C" {

unsigned duckdb_malloc_ncpus() {
#ifdef DUCKDB_NO_THREADS
	return 1
#else
	unsigned concurrency = duckdb::NumericCast<unsigned>(std::thread::hardware_concurrency());
	return std::max(concurrency, 1u);
#endif
}
}

namespace duckdb {

static string PurgeArenaString(idx_t arena_idx) {
	return StringUtil::Format("arena.%llu.purge", arena_idx);
}

static void JemallocCTL(const char *name, void *old_ptr, size_t *old_len, void *new_ptr, size_t new_len) {
	if (duckdb_je_mallctl(name, old_ptr, old_len, new_ptr, new_len) != 0) {
#ifdef DEBUG
		throw InternalException("je_mallctl failed for setting \"%s\"", name);
#endif
	}
}

template <class T>
static void SetJemallocCTL(const char *name, T &val) {
	JemallocCTL(name, nullptr, nullptr, &val, sizeof(T));
}

static void SetJemallocCTL(const char *name) {
	JemallocCTL(name, nullptr, nullptr, nullptr, 0);
}

template <class T>
static T GetJemallocCTL(const char *name) {
	T result;
	size_t len = sizeof(T);
	JemallocCTL(name, &result, &len, nullptr, 0);
	return result;
}

data_ptr_t Allocator::DefaultAllocate(PrivateAllocatorData *private_data, idx_t size) {
	return data_ptr_cast(duckdb_je_malloc(size));
}

void Allocator::DefaultFree(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	duckdb_je_free(pointer);
}

data_ptr_t Allocator::DefaultReallocate(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                        idx_t size) {
	return data_ptr_cast(duckdb_je_realloc(pointer, size));
}

optional_idx Allocator::DecayDelay() {
	return NumericCast<idx_t>(DUCKDB_JEMALLOC_DECAY);
}

bool Allocator::SupportsFlush() {
	return true;
}

void Allocator::ThreadFlush(bool allocator_background_threads, idx_t threshold, idx_t thread_count) {
	if (!allocator_background_threads) {
		// We flush after exceeding the threshold
		if (GetJemallocCTL<uint64_t>("thread.peak.read") <= threshold) {
			return;
		}

		// Flush thread-local cache
		SetJemallocCTL("thread.tcache.flush");

		// Flush this thread's arena
		const auto purge_arena = PurgeArenaString(idx_t(GetJemallocCTL<unsigned>("thread.arena")));
		SetJemallocCTL(purge_arena.c_str());

		// Reset the peak after resetting
		SetJemallocCTL("thread.peak.reset");
	}
}

void Allocator::ThreadIdle() {
	// Indicate that this thread is idle
	SetJemallocCTL("thread.idle");

	// Reset the peak after resetting
	SetJemallocCTL("thread.peak.reset");
}

void Allocator::FlushAll() {
	// Flush thread-local cache
	SetJemallocCTL("thread.tcache.flush");

	// Flush all arenas
	const auto purge_arena = PurgeArenaString(MALLCTL_ARENAS_ALL);
	SetJemallocCTL(purge_arena.c_str());

	// Reset the peak after resetting
	SetJemallocCTL("thread.peak.reset");
}

void Allocator::SetBackgroundThreads(bool enable) {
#ifndef __APPLE__
	SetJemallocCTL("background_thread", enable);
#endif
}

} // namespace duckdb

#endif
