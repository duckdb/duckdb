//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/execution/index/art/fixed_size_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/common/constants.hpp"
#include "duckdb/common/vector.hpp"
#include "duckdb/common/assert.hpp"
#include "duckdb/common/types/validity_mask.hpp"

namespace duckdb {

struct BufferEntry {
	BufferEntry(const data_ptr_t &ptr, const idx_t &allocation_count) : ptr(ptr), allocation_count(allocation_count) {
	}
	data_ptr_t ptr;
	idx_t allocation_count;
};

//! The FixedSizeAllocator provides pointers to fixed-size sections of pre-allocated memory.
//! The pointers are of type idx_t, and the leftmost byte must always be zero.
//! The second to fourth byte store the offset into a buffer, and the last four bytes store the buffer ID,
//! i.e., a position looks like this [0: empty, 1  - 3: offset, 4 - 7: buffer ID].
class FixedSizeAllocator {
public:
	//! Bitwise AND hex values
	static constexpr idx_t BUFFER_ID_TO_ZERO = 0xFFFFFFFF00000000;
	static constexpr idx_t OFFSET_TO_ZERO = 0xFF000000FFFFFFFF;
	static constexpr idx_t OFFSET_AND_FIRST_BYTE_TO_ZERO = 0x00000000FFFFFFFF;
	static constexpr idx_t BUFFER_ID_AND_OFFSET_TO_ZERO = 0xFF00000000000000;
	static constexpr idx_t FIRST_BYTE_TO_ZERO = 0x00FFFFFFFFFFFFFF;

	//! Other constants
	static constexpr idx_t BUFFER_ALLOCATION_SIZE = Storage::BLOCK_ALLOC_SIZE;
	static constexpr uint8_t OFFSET_SHIFT = sizeof(uint8_t) * 8 * 4;
	static constexpr uint8_t VACUUM_THRESHOLD = 4;

	//! Constants for offset calculations
	static constexpr idx_t BASE[] = {0x00000000FFFFFFFF, 0x0000FFFF, 0x00FF, 0x0F, 0x3, 0x1};
	static constexpr uint8_t SHIFT[] = {32, 16, 8, 4, 2, 1};

public:
	explicit FixedSizeAllocator(const idx_t &allocation_size);
	~FixedSizeAllocator();

	//! Allocation size of one element in a buffer
	idx_t allocation_size;
	//! Total number of allocations
	idx_t total_allocations;
	//! Number of validity_t values in the bitmask
	idx_t bitmask_count;
	//! First starting byte of the payload
	idx_t allocation_offset;
	//! Number of possible allocations per buffer
	idx_t allocations_per_buffer;

	//! Buffers containing the data
	vector<BufferEntry> buffers;
	//! Buffers with free space
	unordered_set<idx_t> buffers_with_free_space;

	//! Minimum buffer ID of buffers that can be vacuumed
	idx_t vacuum_threshold;

public:
	//! Get a new position to data, might cause a new buffer allocation
	void New(idx_t &new_position);
	inline idx_t New() {
		idx_t position;
		New(position);
		return position;
	}
	//! Free the data at position
	void Free(const idx_t &position);
	//! Get the data at position
	template <class T>
	inline T *Get(const idx_t &position) const {
		return (T *)Get(position);
	};

	//! Merge another FixedSizeAllocator with this allocator. Both must have the same allocation size
	void Merge(FixedSizeAllocator &other);

	//! Initialize a vacuum operation, and return true, if the allocator needs a vacuum
	bool InitializeVacuum();
	//! Finalize a vacuum operation by freeing all buffers exceeding vacuum_threshold
	void FinalizeVacuum();
	//! Returns true, if a position qualifies for a vacuum operation, and false otherwise
	inline bool NeedsVacuum(const idx_t &position) const {
		// get the buffer ID
		D_ASSERT((position & BUFFER_ID_AND_OFFSET_TO_ZERO) == 0);
		auto buffer_id = position & OFFSET_AND_FIRST_BYTE_TO_ZERO;

		if (buffer_id >= vacuum_threshold) {
			return true;
		}
		return false;
	}
	//! Vacuums a position
	idx_t Vacuum(const idx_t &position);

private:
	//! Returns a data_ptr_t to the position
	inline data_ptr_t Get(const idx_t &position) const {
		D_ASSERT((position & BUFFER_ID_AND_OFFSET_TO_ZERO) == 0);
		D_ASSERT((position & OFFSET_AND_FIRST_BYTE_TO_ZERO) < buffers.size());
		D_ASSERT((position >> OFFSET_SHIFT) < allocations_per_buffer);

		auto buffer_id = (position & OFFSET_AND_FIRST_BYTE_TO_ZERO);
		auto offset = (position >> OFFSET_SHIFT) * allocation_size + allocation_offset;
		return buffers[buffer_id].ptr + offset;
	}
	//! Returns the first free offset in a bitmask
	idx_t GetOffset(ValidityMask &mask, const idx_t &allocation_count);
};

} // namespace duckdb
