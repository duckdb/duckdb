#define DUCKDB_EXTENSION_MAIN
#include "jemalloc_extension.hpp"

#include "jemalloc/jemalloc.h"

namespace duckdb {

void JemallocExtension::Load(DuckDB &db) {
	// NOP: This extension can only be loaded statically
}

std::string JemallocExtension::Name() {
	return "jemalloc";
}

static void JemallocCTL(const char *name, void *old_ptr, size_t *old_len, void *new_ptr, size_t new_len) {
	if (duckdb_je_mallctl(name, old_ptr, old_len, new_ptr, new_len) != 0) {
#ifdef DEBUG
		// We only want to throw an exception here when debugging
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

static inline string PurgeArenaString(idx_t arena_idx) {
	return StringUtil::Format("arena.%llu.purge", arena_idx);
}

int64_t JemallocExtension::DecayDelay() {
	return DUCKDB_JEMALLOC_DECAY;
}

void JemallocExtension::ThreadFlush(idx_t threshold) {
	// We flush after exceeding the threshold
	if (GetJemallocCTL<uint64_t>("thread.peak.read") > threshold) {
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

void JemallocExtension::ThreadIdle() {
	// Indicate that this thread is idle
	SetJemallocCTL("thread.idle");

	// Reset the peak after resetting
	SetJemallocCTL("thread.peak.reset");
}

void JemallocExtension::FlushAll() {
	// Flush all arenas
	const auto purge_arena = PurgeArenaString(MALLCTL_ARENAS_ALL);
	SetJemallocCTL(purge_arena.c_str());
}

void JemallocExtension::SetBackgroundThreads(bool enable) {
#ifndef __APPLE__
	SetJemallocCTL("background_thread", enable);
#endif
}

std::string JemallocExtension::Version() const {
#ifdef EXT_VERSION_JEMALLOC
	return EXT_VERSION_JEMALLOC;
#else
	return "";
#endif
}

} // namespace duckdb

extern "C" {

DUCKDB_EXTENSION_API void jemalloc_init(duckdb::DatabaseInstance &db) {
	duckdb::DuckDB db_wrapper(db);
	db_wrapper.LoadExtension<duckdb::JemallocExtension>();
}

DUCKDB_EXTENSION_API const char *jemalloc_version() {
	return duckdb::DuckDB::LibraryVersion();
}
}

#ifndef DUCKDB_EXTENSION_MAIN
#error DUCKDB_EXTENSION_MAIN not defined
#endif
