//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_states.hpp"
#include "tuple_data_layout.hpp"

namespace duckdb {

struct TupleDataSegment {
public:
	TupleDataSegment(uint32_t row_block_index_p, idx_t row_block_offset_p, uint32_t heap_block_index_p,
	                 idx_t heap_block_offset_p, idx_t last_heap_row_size_p, uint32_t count_p)
	    : row_block_index(row_block_index_p), row_block_offset(row_block_offset_p),
	      heap_block_index(heap_block_index_p), heap_block_offset(heap_block_offset_p),
	      last_heap_row_size(last_heap_row_size_p), count(count_p) {
	}

	//! Disable copy constructors
	TupleDataSegment(const TupleDataSegment &other) = delete;
	TupleDataSegment &operator=(const TupleDataSegment &) = delete;

	//! Enable move constructors
	TupleDataSegment(TupleDataSegment &&other) noexcept;
	TupleDataSegment &operator=(TupleDataSegment &&) noexcept;

public:
	//! Index/offset of the row block
	uint32_t row_block_index;
	idx_t row_block_offset;
	//! Index/offset of the heap block
	uint32_t heap_block_index;
	idx_t heap_block_offset;
	//! Size of the last heap row (for all but the last we can infer the size from the pointer difference)
	idx_t last_heap_row_size;
	//! Tuple count for this segment
	uint32_t count;
};

struct TupleDataBlock {
public:
	explicit TupleDataBlock(idx_t capacity_p) : capacity(capacity_p) {
	}

	//! Disable copy constructors
	TupleDataBlock(const TupleDataBlock &other) = delete;
	TupleDataBlock &operator=(const TupleDataBlock &) = delete;

	//! Enable move constructors
	TupleDataBlock(TupleDataBlock &&other) noexcept;
	TupleDataBlock &operator=(TupleDataBlock &&) noexcept;

public:
	//! Remaining capacity (in bytes)
	idx_t RemainingCapacity() {
		D_ASSERT(size <= capacity);
		return capacity - size;
	}

	//! Remaining capacity (in rows)
	idx_t RemainingCapacity(idx_t row_width) {
		return RemainingCapacity() / row_width;
	}

public:
	//! The underlying row block
	shared_ptr<BlockHandle> handle;
	//! Capacity (in bytes)
	idx_t capacity;
	//! Occupied size (in bytes)
	idx_t size;
};

class TupleDataAllocator {
public:
	explicit TupleDataAllocator(ClientContext &context, const TupleDataLayout &layout);

public:
	//! Builds out the segments for next append, given the metadata in the append state
	void Build(TupleDataAppendState &append_state, idx_t count, vector<TupleDataSegment> &segments);
	//! Gets the base pointer to the rows for the given segment
	data_ptr_t GetRowPointer(TupleDataManagementState &state, const TupleDataSegment &segment);
	//! Gets the base pointer to the heap for the given segment
	data_ptr_t GetHeapPointer(TupleDataManagementState &state, const TupleDataSegment &segment);

private:
	//! Builds out a single segment (grabs the lock)
	TupleDataSegment BuildSegment(TupleDataAppendState &append_state, idx_t offset, idx_t count);
	//! Pins the given row block
	void PinRowBlock(TupleDataManagementState &state, const uint32_t row_block_index);
	//! Pins the given heap block
	void PinHeapBlock(TupleDataManagementState &state, const uint32_t heap_block_index);

private:
	//! The lock (for shared allocations)
	mutex lock;
	//! The buffer manager
	BufferManager &buffer_manager;
	//! The layout of the data
	const TupleDataLayout &layout;
	//! Blocks storing the fixed-size rows
	vector<TupleDataBlock> row_blocks;
	//! Blocks storing the variable-size data of the fixed-size rows (e.g., string, list)
	vector<TupleDataBlock> heap_blocks;
};

} // namespace duckdb
