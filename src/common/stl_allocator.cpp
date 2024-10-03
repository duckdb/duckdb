#include "duckdb/common/stl_allocator.hpp"

#ifdef USE_JEMALLOC
#include "jemalloc/jemalloc/include/jemalloc/jemalloc.h"
#endif

namespace duckdb {

void *stl_malloc(size_t size) {
#ifdef USE_JEMALLOC
	return duckdb_je_malloc(size);
#else
	return malloc(size);
#endif
}

DUCKDB_API void *stl_realloc(void *ptr, size_t size) {
#ifdef USE_JEMALLOC
	return duckdb_je_realloc(ptr, size);
#else
	return realloc(ptr, size);
#endif
}

void stl_free(void *ptr) {
#ifdef USE_JEMALLOC
	duckdb_je_free(ptr);
#else
	free(ptr);
#endif
}

} // namespace duckdb
