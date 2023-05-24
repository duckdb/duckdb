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
	return (data_ptr_t)duckdb_jemalloc::je_malloc(size);
}

void JEMallocExtension::Free(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	duckdb_jemalloc::je_free(pointer);
}

data_ptr_t JEMallocExtension::Reallocate(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                         idx_t size) {
	return (data_ptr_t)duckdb_jemalloc::je_realloc(pointer, size);
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

template <>
string GetJemallocCTL(const char *name) {
	const char *data;
	size_t len = sizeof(data);
	JemallocCTL(name, &data, &len, nullptr, 0);
	return string(data, len);
}

void JEMallocExtension::Configure() {
	if (duckdb_jemalloc::je_malloc_conf != nullptr) {
		throw InternalException("oops");
	}
	duckdb_jemalloc::je_malloc_conf = "oversize_threshold:0";

	auto a = Allocate(nullptr, 4);
	Free(nullptr, a, 4);

	if (GetJemallocCTL<size_t>("opt.oversize_threshold") != 0) {
		throw InternalException("couldn't set");
	}

	// First we try the percpu option
	//	duckdb_jemalloc::je_malloc_conf = "percpu_arena:percpu,oversize_threshold:0";
	//	if (GetJemallocCTL<string>("opt.percpu_arena") == "percpu") {
	//		return;
	//	}

	// We weren't successful, just set the number of arenas to the number of threads
	//	const idx_t physical_cores = std::thread::hardware_concurrency();
	//	const auto conf_str = StringUtil::Format("narenas:%llu,oversize_threshold:0", physical_cores);
	//	duckdb_jemalloc::je_malloc_conf = conf_str.c_str();
	//	if (GetJemallocCTL<unsigned>("opt.narenas") != physical_cores) {
	//		throw InternalException("oops");
	//	}
}

void JEMallocExtension::ThreadIdle() {
	SetJemallocCTL("thread.idle");
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
