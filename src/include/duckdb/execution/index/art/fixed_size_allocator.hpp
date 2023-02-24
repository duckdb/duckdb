//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/fixed_size_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {

// TODO: when do I want to perform 'clean-ups'? At a certain free-list size?
// TODO: make sure that the destructor cleans up all the memory

class FixedSizeAllocator {
public:
	FixedSizeAllocator(idx_t alloc_size);
	~FixedSizeAllocator();

	//! Get the data at position
	template <class T>
	T *GetDataAtPosition(const idx_t position);
	//! Get a new position to data, might cause a new buffer allocation
	idx_t GetPosition();
	//! Free the data at position, i.e., add the position to the free list
	void FreePosition(const idx_t position);

private:
	vector<data_ptr_t> buffers;
	vector<idx_t> free_list;

	idx_t alloc_size;
	idx_t positions_per_buffer;
};

} // namespace duckdb
