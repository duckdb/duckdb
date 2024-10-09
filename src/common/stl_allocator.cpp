#include "duckdb/common/stl_allocator.hpp"

#include "duckdb/common/assert.hpp"

#ifdef USE_JEMALLOC
#include "jemalloc_extension.hpp"
#endif

#include <cstdlib>

namespace duckdb {

static AllocationFunctions GetDefaultAllocationFunctions() {
	AllocationFunctions result(nullptr, nullptr, nullptr);
#ifdef USE_JEMALLOC
	result = JemallocExtension::GetAllocationFunctions();
#else
	result = AllocationFunctions(malloc, realloc, free);
#endif
	// If DEFAULT_ALLOCATION_FUNCTIONS was somehow already set, make sure we're setting the same function pointers
	D_ASSERT(DEFAULT_ALLOCATION_FUNCTIONS.malloc == nullptr ||
	         (DEFAULT_ALLOCATION_FUNCTIONS.malloc == result.malloc &&
	          DEFAULT_ALLOCATION_FUNCTIONS.realloc == result.realloc &&
	          DEFAULT_ALLOCATION_FUNCTIONS.free == result.free));
	return result;
}

AllocationFunctions DEFAULT_ALLOCATION_FUNCTIONS = GetDefaultAllocationFunctions(); // NOLINT: non-const on purpose

} // namespace duckdb
