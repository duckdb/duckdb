#include "duckdb/common/stl_allocator.hpp"

#ifdef USE_JEMALLOC
#include "jemalloc_extension.hpp"
#endif

namespace duckdb {

AllocationFunctions GetDefaultAllocationFunctions() {
	// This should be called exactly once, to initialize DEFAULT_ALLOCATION_FUNCTIONS only
	D_ASSERT(DEFAULT_ALLOCATION_FUNCTIONS.malloc == nullptr && DEFAULT_ALLOCATION_FUNCTIONS.realloc == nullptr &&
	         DEFAULT_ALLOCATION_FUNCTIONS.free == nullptr);
#ifdef USE_JEMALLOC
	return JemallocExtension::GetAllocationFunctions();
#else
	return AllocationFunctions(malloc, realloc, free);
#endif
}

} // namespace duckdb
