#pragma once

#include "jemalloc/jemalloc.h"

namespace duckdb {

class JEMallocWrapper {
public:
	static inline void *Allocate(idx_t size) {
		return duckdb_jemalloc::je_malloc(size);
	}

	static inline void Free(void *ptr) {
		duckdb_jemalloc::je_free(ptr);
	}

	static inline void *ReAllocate(void *ptr, idx_t size) {
		return duckdb_jemalloc::je_realloc(ptr, size);
	}
};

} // namespace duckdb
