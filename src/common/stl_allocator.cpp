#include "duckdb/common/stl_allocator.hpp"

#include "duckdb/common/allocator.hpp"

namespace duckdb {

void *stl_malloc(size_t size) {
	return Allocator::DefaultAllocate(nullptr, size);
}

void stl_free(void *ptr) {
	return Allocator::DefaultFree(nullptr, data_ptr_cast(ptr), 0);
}

} // namespace duckdb
