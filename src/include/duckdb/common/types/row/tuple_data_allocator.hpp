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
struct TupleDataPinState;
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
	void Build(TupleDataSegment &segment, TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
	           idx_t initial_offset, idx_t count);
	//! Initializes a chunk, making its pointers valid
	void InitializeChunkState(TupleDataSegment &segment, TupleDataPinState &pin_state, TupleDataChunkState &chunk_state,
	                          idx_t chunk_idx, bool init_heap);
	static inline void RecomputeHeapPointers(Vector &old_heap_ptrs, const SelectionVector &old_heap_sel,
	                                         const data_ptr_t row_locations[], Vector &new_heap_ptrs,
	                                         const idx_t offset, const idx_t count, const TupleDataLayout &layout,
	                                         const idx_t base_col_offset);
	//! Releases or stores any handles in the management state that are no longer required
	void ReleaseOrStoreHandles(TupleDataPinState &state, TupleDataSegment &segment, TupleDataChunk &chunk);
	//! Releases or stores ALL handles in the management state
	void ReleaseOrStoreHandles(TupleDataPinState &state, TupleDataSegment &segment);

private:
	//! Builds out a single part (grabs the lock)
	TupleDataChunkPart BuildChunkPart(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state, idx_t offset,
	                                  idx_t count);
	//! Internal function for InitializeChunkState
	void InitializeChunkStateInternal(TupleDataPinState &pin_state, TupleDataChunkState &chunk_state, idx_t offset,
	                                  bool init_heap_pointers, bool init_heap_sizes,
	                                  vector<TupleDataChunkPart *> &parts);
	//! Internal function for ReleaseOrStoreHandles
	static void ReleaseOrStoreHandlesInternal(TupleDataSegment &segment, unordered_map<uint32_t, BufferHandle> &handles,
	                                          const unordered_set<uint32_t> &block_ids, vector<TupleDataBlock> &blocks,
	                                          TupleDataPinProperties properties);
	//! Pins the given row block
	void PinRowBlock(TupleDataPinState &state, const uint32_t row_block_index);
	//! Pins the given heap block
	void PinHeapBlock(TupleDataPinState &state, const uint32_t heap_block_index);
	//! Gets the pointer to the rows for the given chunk part
	data_ptr_t GetRowPointer(TupleDataPinState &state, const TupleDataChunkPart &part);
	//! Gets the base pointer to the heap for the given chunk part
	data_ptr_t GetBaseHeapPointer(TupleDataPinState &state, const TupleDataChunkPart &part);

private:
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
