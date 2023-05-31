#define DUCKDB_EXTENSION_MAIN
#include "jemalloc-extension.hpp"

#include "duckdb/common/allocator.hpp"
#include "jemalloc/jemalloc.h"

#ifndef DUCKDB_NO_THREADS
#include "duckdb/common/thread.hpp"
#endif

namespace duckdb {

void JEMallocExtension::Load(DuckDB &db) {
	// NOP: This extension can only be loaded statically
}

std::string JEMallocExtension::Name() {
	return "jemalloc";
}

data_ptr_t JEMallocExtension::Allocate(PrivateAllocatorData *private_data, idx_t size) {
	return data_ptr_cast(duckdb_jemalloc::je_malloc(size));
}

void JEMallocExtension::Free(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	duckdb_jemalloc::je_free(pointer);
}

data_ptr_t JEMallocExtension::Reallocate(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                         idx_t size) {
	return data_ptr_cast(duckdb_jemalloc::je_realloc(pointer, size));
}

static void JemallocCTL(const char *name, void *old_ptr, size_t *old_len, void *new_ptr, size_t new_len) {
	if (duckdb_jemalloc::je_mallctl(name, old_ptr, old_len, new_ptr, new_len) != 0) {
		throw InternalException("je_mallctl failed for setting \"%s\"", name);
	}
}

template <class T>
static void SetJemallocCTL(const char *name, T &val) {
	JemallocCTL(name, &val, sizeof(T));
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

void JEMallocExtension::ThreadFlush() {
	// Flush threshold is 1 << 28 (~256MB)
	// For 1 << 27 we regress on benchmark/imdb/07c.benchmark
	static constexpr uint64_t FLUSH_THRESHOLD = 268435456;

	// We flush after exceeding the threshold
	if (GetJemallocCTL<uint64_t>("thread.peak.read") < FLUSH_THRESHOLD) {
		return;
	}

	// Flush thread-local cache
	SetJemallocCTL("thread.tcache.flush");

	// Flush this thread's arena
	const auto purge_arena = StringUtil::Format("arena.%llu.purge", idx_t(GetJemallocCTL<unsigned>("thread.arena")));
	SetJemallocCTL(purge_arena.c_str());

	// Reset the peak after resetting
	SetJemallocCTL("thread.peak.reset");
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void jemalloc_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::JEMallocExtension>();
}

DUCKDB_EXTENSION_API const char *jemalloc_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
