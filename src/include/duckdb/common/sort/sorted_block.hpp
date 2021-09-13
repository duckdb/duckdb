//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sort/sorted_block.hpp
//
//
//===----------------------------------------------------------------------===//
#pragma once

#include "duckdb/common/types/row_layout.hpp"

namespace duckdb {

class BufferManager;
struct RowDataBlock;
struct SortLayout;
struct GlobalSortState;

//! Object that holds sorted rows, and an accompanying heap if there are blobs
struct SortedData {
public:
	SortedData(const RowLayout &layout, BufferManager &buffer_manager, GlobalSortState &state);
	//! Number of rows that this object holds
	idx_t Count();
	//! Pin the current block so it can be read
	void Pin();
	//! Pointer to the row that is currently being read from
	data_ptr_t DataPtr() const;
	//! Pointer to the heap row that corresponds to the current row
	data_ptr_t HeapPtr() const;
	//! Advance one row
	void Advance(const bool &adv);
	//! Initialize new block to write to
	void CreateBlock();
	//! Reset read indices to the given indices
	void ResetIndices(idx_t block_idx_to, idx_t entry_idx_to);
	//! Create a slice that holds the rows between the start and end indices
	unique_ptr<SortedData> CreateSlice(idx_t start_block_index, idx_t start_entry_index, idx_t end_block_index,
	                                   idx_t end_entry_index);
	//! Unswizzles all
	void Unswizzle();

public:
	//! Layout of this data
	const RowLayout layout;
	//! Data and heap blocks
	vector<RowDataBlock> data_blocks;
	vector<RowDataBlock> heap_blocks;
	//! Buffer handles to the data being currently read
	unique_ptr<BufferHandle> data_handle;
	unique_ptr<BufferHandle> heap_handle;
	//! Read indices
	idx_t block_idx;
	idx_t entry_idx;
	//! Whether the pointers in this sorted data are swizzled
	bool swizzled;

private:
	//! Pin fixed-size row data
	void PinData();
	//! Pin the accompanying heap data (if any)
	void PinHeap();

private:
	//! The buffer manager
	BufferManager &buffer_manager;
	//! The global state
	GlobalSortState &state;
	//! Pointers into the buffers being currently read
	data_ptr_t data_ptr;
	data_ptr_t heap_ptr;
};

//! Block that holds sorted rows: radix, blob and payload data
struct SortedBlock {
public:
	SortedBlock(BufferManager &buffer_manager, GlobalSortState &gstate);
	//! Number of rows that this object holds
	idx_t Count() const;
	//! The remaining number of rows to be read from this object
	idx_t Remaining() const;
	//! Initialize this block to write data to
	void InitializeWrite();
	//! Init new block to write to
	void CreateBlock();
	//! Pins radix block with given index
	void PinRadix(idx_t pin_block_idx);
	//! Fill this sorted block by appending the blocks held by a vector of sorted blocks
	void AppendSortedBlocks(vector<unique_ptr<SortedBlock>> &sorted_blocks);
	//! Locate the block and entry index of a row in this block,
	//! given an index between 0 and the total number of rows in this block
	void GlobalToLocalIndex(const idx_t &global_idx, idx_t &local_block_index, idx_t &local_entry_index);
	//! Create a slice that holds the rows between the start and end indices
	unique_ptr<SortedBlock> CreateSlice(const idx_t start, const idx_t end);

	//! Size (in bytes) of the heap of this block
	idx_t HeapSize() const;
	//! Total size (in bytes) of this block
	idx_t SizeInBytes() const;

public:
	//! Radix/memcmp sortable data
	vector<RowDataBlock> radix_sorting_data;
	unique_ptr<BufferHandle> radix_handle;
	idx_t block_idx;
	idx_t entry_idx;
	//! Variable sized sorting data
	unique_ptr<SortedData> blob_sorting_data;
	//! Payload data
	unique_ptr<SortedData> payload_data;

private:
	//! Buffer manager, and sorting layout constants
	BufferManager &buffer_manager;
	GlobalSortState &state;
	const SortLayout &sort_layout;
	const RowLayout &payload_layout;
};

struct SortedDataScanner {
public:
	SortedDataScanner(SortedData &sorted_data, GlobalSortState &global_sort_state);

	//! Scans the next data chunk from the sorted data
	void Scan(DataChunk &chunk);

private:
	//! The sorted data being scanned
	SortedData &sorted_data;
	//! The total count of sorted_data
	const idx_t total_count;
	//! The global sort state
	GlobalSortState &global_sort_state;
	//! Addresses used to gather from the sorted data
	Vector addresses = Vector(LogicalType::POINTER);
	//! The number of rows scanned so far
	idx_t total_scanned;
};

} // namespace duckdb
