#include "duckdb/common/types/string_heap.hpp"

#include "duckdb/common/types/string_type.hpp"
#include "duckdb/common/algorithm.hpp"
#include "duckdb/common/exception.hpp"
#include "utf8proc_wrapper.hpp"

#include <cstring>

namespace duckdb {

StringHeap::StringHeap(Allocator &allocator) : allocator(allocator) {
}

void StringHeap::Destroy() {
	allocator.Destroy();
}

void StringHeap::Move(StringHeap &other) {
	other.allocator.Move(allocator);
}

idx_t StringHeap::SizeInBytes() const {
	return allocator.SizeInBytes();
}

idx_t StringHeap::AllocationSize() const {
	return allocator.AllocationSize();
}

} // namespace duckdb
