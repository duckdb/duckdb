//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "tuple_data_layout.hpp"

namespace duckdb {

struct TupleDataChunk;
struct TupleDataSegment;
struct TupleDataManagementState;
struct TupleDataAppendState;

struct TupleDataBlock {
public:
	TupleDataBlock(BufferManager &buffer_manager, idx_t capacity_p);

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
	void Build(TupleDataAppendState &append_state, idx_t count, TupleDataSegment &segment);
	//! Gets the base pointer to the rows for the given segment
	data_ptr_t GetRowPointer(TupleDataManagementState &state, const TupleDataChunk &segment);
	//! Gets the base pointer to the heap for the given segment
	data_ptr_t GetHeapPointer(TupleDataManagementState &state, const TupleDataChunk &segment);

private:
	//! Builds out a single segment (grabs the lock)
	TupleDataChunk BuildChunk(TupleDataAppendState &append_state, idx_t offset, idx_t count);
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
