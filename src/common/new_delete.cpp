#include "duckdb/common/new_delete.hpp"

#include "duckdb/common/allocator.hpp"

namespace duckdb {

data_ptr_t AllocatorWrapper::Allocate(idx_t size) {
	return Allocator::DefaultAllocate(nullptr, size);
}

void AllocatorWrapper::Free(data_ptr_t pointer) {
	Allocator::DefaultFree(nullptr, pointer, 0);
}

} // namespace duckdb
