//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/fixed_size_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/pair.hpp"
#include "duckdb/common/set.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/list.hpp"

namespace duckdb {

//! The FixedSizeAllocator provides pointers to fixed-size sections of pre-allocated memory.
//! The pointers are of type idx_t, and the leftmost byte must always be zero.
//! The second to fourth byte store the offset into a buffer, and the last four bytes store the buffer ID,
//! i.e., a position looks like this [0: empty, 1  - 3: offset, 4 - 7: buffer ID].
class FixedSizeAllocator {
public:
	static constexpr idx_t BUFFER_ID_TO_ZERO = 0xFFFFFFFF00000000;
	static constexpr idx_t OFFSET_TO_ZERO = 0xFF000000FFFFFFFF;
	static constexpr idx_t OFFSET_AND_FIRST_BYTE_TO_ZERO = 0x00000000FFFFFFFF;
	static constexpr idx_t BUFFER_ID_AND_OFFSET_TO_ZERO = 0xFF00000000000000;
	static constexpr idx_t FIRST_BYTE_TO_ZERO = 0x00FFFFFFFFFFFFFF;

public:
	explicit FixedSizeAllocator(const idx_t &allocation_size);
	~FixedSizeAllocator();

	//! Allocation size of one element in a buffer
	idx_t allocation_size;
	//! Number of offsets into a buffer
	idx_t offsets_per_buffer;
	//! Buffers containing the data
	vector<data_ptr_t> buffers;
	//! List of lists containing all free positions, with buffer ID offsets
	list<pair<idx_t, list<idx_t>>> free_list;

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

	//! Initializes a vacuum operation, and returns true, if the allocator requires a vacuum
	bool InitializeVacuum();
	//! Finalizes a vacuum operation by calling the finalize operation of the respective
	//! fixed size allocators
	void FinalizeVacuum();
	//! Returns true, if the position qualifies for a vacuum operation
	bool NeedsVacuum(const idx_t &position) const;
	//! Vacuums a position and returns the new position
	idx_t Vacuum(const idx_t &position);

private:
	//! Keeps track of the buffer id threshold for vacuum operations. This is
	//! set once when starting a vacuum operation.
	idx_t vacuum_threshold;

	//! Returns a data_ptr_t to the position
	data_ptr_t Get(const idx_t &position) const;
	//! Ensures that the first free list in the list always has a buffer ID offset
	//! of zero
	void RearrangeFreeList();
};

} // namespace duckdb
