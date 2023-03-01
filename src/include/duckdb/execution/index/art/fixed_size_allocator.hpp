//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/fixed_size_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/queue.hpp"

namespace duckdb {

//! Custom comparison function for the positions in the free list (priority queue) of the fixed size allocator
struct CustomLess {
	bool operator()(const idx_t left, const idx_t right) const {
		if ((left & 0x0000ffff) > (right & 0x0000ffff)) {
			return false;
		}
		return ((left & 0x0fff0000) > (right & 0x0fff0000));
	}
};

//! The FixedSizeAllocator provides pointers to fixed-size sections of pre-allocated memory.
//! The pointers are of type idx_t, and the leftmost byte must always be zero.
//! The second to fourth byte store the offset into a buffer, and the last four bytes store the buffer ID,
//! i.e., a position looks like this [0: empty, 1  - 3: offset, 4 - 7: buffer ID].
class FixedSizeAllocator {
public:
	explicit FixedSizeAllocator(const idx_t &allocation_size);
	~FixedSizeAllocator();

	//! Allocation size of one element in a buffer
	idx_t allocation_size;
	//! Number of offsets into a buffer
	idx_t offsets_per_buffer;
	//! Buffers containing the data
	vector<data_ptr_t> buffers;
	//! Minimum priority queue containing all free positions
	priority_queue<idx_t, vector<idx_t>, CustomLess> free_list;

public:
	//! Get a new position to data, might cause a new buffer allocation
	idx_t New();
	//! Free the data at position, i.e., add the position to the free list
	void Free(const idx_t &position);
	//! Get the data at position
	template <class T>
	T *Get(const idx_t &position) const;

	//! Merge another FixedSizeAllocator with this allocator. Both must have the same allocation size
	void Merge(FixedSizeAllocator &other);
	//! VacuumCount returns the number of buffers that can be vacuumed
	idx_t VacuumCount();
	//! Vacuums a position (if necessary), and returns the new position
	idx_t Vacuum(const idx_t &position);

private:
	//! Returns a data_ptr_t to the position
	data_ptr_t Get(const idx_t &position) const;
};

} // namespace duckdb
