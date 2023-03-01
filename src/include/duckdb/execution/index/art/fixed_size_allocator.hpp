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
	explicit FixedSizeAllocator(idx_t alloc_size);
	~FixedSizeAllocator();

	//! Get a new position to data, might cause a new buffer allocation
	idx_t New();
	//! Free the data at position, i.e., add the position to the free list
	void Free(const idx_t &position);
	//! Get the data at position
	template <class T>
	T *Get(const idx_t &position) const;

private:
	vector<data_ptr_t> buffers;
	vector<idx_t> free_list;

	idx_t alloc_size;
	idx_t positions_per_buffer;
};

} // namespace duckdb
