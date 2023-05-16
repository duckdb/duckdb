#define DUCKDB_EXTENSION_MAIN
#include "jemalloc_extension.hpp"

#include "duckdb/common/allocator.hpp"
#include "jemalloc/jemalloc.h"

namespace duckdb {

void JemallocExtension::Load(DuckDB &db) {
	// NOP: This extension can only be loaded statically
}

std::string JemallocExtension::Name() {
	return "jemalloc";
}

data_ptr_t JemallocExtension::Allocate(PrivateAllocatorData *private_data, idx_t size) {
	return (data_ptr_t)duckdb_jemalloc::je_malloc(size);
}

void JemallocExtension::Free(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t size) {
	duckdb_jemalloc::je_free(pointer);
}

data_ptr_t JemallocExtension::Reallocate(PrivateAllocatorData *private_data, data_ptr_t pointer, idx_t old_size,
                                         idx_t size) {
	return (data_ptr_t)duckdb_jemalloc::je_realloc(pointer, size);
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
