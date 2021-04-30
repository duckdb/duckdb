#include "duckdb/common/allocator.hpp"

namespace duckdb {

AllocatedData::AllocatedData(Allocator &allocator, data_ptr_t pointer) : allocator(allocator), pointer(pointer) {
}
AllocatedData::~AllocatedData() {
	Reset();
}

void AllocatedData::Reset() {
	if (!pointer) {
		return;
	}
	allocator.FreeData(pointer);
	pointer = nullptr;
}

Allocator::Allocator() : allocate_function(Allocator::DefaultAllocate), free_function(Allocator::DefaultFree) {
}

Allocator::Allocator(allocate_function_ptr_t allocate_function_p, free_function_ptr_t free_function_p,
                     unique_ptr<PrivateAllocatorData> private_data)
    : allocate_function(allocate_function_p), free_function(free_function_p), private_data(move(private_data)) {
}

data_ptr_t Allocator::AllocateData(idx_t size) {
	return allocate_function(private_data.get(), size);
}

void Allocator::FreeData(data_ptr_t pointer) {
	if (!pointer) {
		return;
	}
	return free_function(private_data.get(), pointer);
}

} // namespace duckdb
