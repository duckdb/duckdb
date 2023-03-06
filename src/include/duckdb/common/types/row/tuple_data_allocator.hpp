//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/types/row/tuple_data_allocator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/types/row/tuple_data_layout.hpp"
#include "duckdb/common/types/row/tuple_data_states.hpp"

namespace duckdb {

struct TupleDataSegment;
struct TupleDataChunk;
struct TupleDataChunkPart;
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
	explicit TupleDataAllocator(BufferManager &buffer_manager, const TupleDataLayout &layout);

	//! Get the buffer allocator
	Allocator &GetAllocator();
	//! Get the layout
	const TupleDataLayout &GetLayout();

public:
	//! Builds out the chunks for next append, given the metadata in the append state
	void Build(TupleDataAppendState &append_state, idx_t count, TupleDataSegment &segment);
	//! Initializes a chunk, making its pointers valid
	void InitializeChunkState(TupleDataManagementState &state, TupleDataSegment &segment, idx_t chunk_idx,
	                          bool init_heap);
	//! Releases or stores any handles that are no longer required
	void ReleaseOrStoreHandles(TupleDataManagementState &state, TupleDataSegment &segment, TupleDataChunk &chunk) const;

private:
	//! Builds out a single part (grabs the lock)
	TupleDataChunkPart BuildChunkPart(TupleDataManagementState &state, idx_t offset, idx_t count);
	//! Internal function for InitializeChunkState
	void InitializeChunkStateInternal(TupleDataManagementState &state, bool init_heap_pointers, bool init_heap_sizes,
	                                  vector<TupleDataChunkPart *> &parts);
	//! Recomputes the heap pointers if the heap block changed
	static void RecomputeHeapPointers(const data_ptr_t old_base_heap_ptr, const data_ptr_t new_base_heap_ptr,
	                                  const data_ptr_t row_locations[], const idx_t offset, const idx_t count,
	                                  const TupleDataLayout &layout, const idx_t base_col_offset);
	//! Internal function for ReleaseOrStoreHandles
	static void ReleaseOrStoreHandlesInternal(unordered_map<uint32_t, BufferHandle> &handles,
	                                          const unordered_set<uint32_t> &block_ids, TupleDataSegment &segment,
	                                          TupleDataPinProperties properties);
	//! Pins the given row block
	void PinRowBlock(TupleDataManagementState &state, const uint32_t row_block_index);
	//! Pins the given heap block
	void PinHeapBlock(TupleDataManagementState &state, const uint32_t heap_block_index);
	//! Gets the pointer to the rows for the given chunk part
	data_ptr_t GetRowPointer(TupleDataManagementState &state, const TupleDataChunkPart &part);
	//! Gets the base pointer to the heap for the given chunk part
	data_ptr_t GetBaseHeapPointer(TupleDataManagementState &state, const TupleDataChunkPart &part);

private:
	//! The lock (for shared allocations)
	mutex lock;
	//! The buffer manager
	BufferManager &buffer_manager;
	//! The layout of the data
	const TupleDataLayout layout;
	//! Blocks storing the fixed-size rows
	vector<TupleDataBlock> row_blocks;
	//! Blocks storing the variable-size data of the fixed-size rows (e.g., string, list)
	vector<TupleDataBlock> heap_blocks;
};

} // namespace duckdb
