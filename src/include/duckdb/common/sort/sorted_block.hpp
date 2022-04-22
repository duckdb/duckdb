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

enum class SortedDataType { BLOB, PAYLOAD };

//! Object that holds sorted rows, and an accompanying heap if there are blobs
struct SortedData {
public:
	SortedData(SortedDataType type, const RowLayout &layout, BufferManager &buffer_manager, GlobalSortState &state);
	//! Number of rows that this object holds
	idx_t Count();
	//! Initialize new block to write to
	void CreateBlock();
	//! Create a slice that holds the rows between the start and end indices
	unique_ptr<SortedData> CreateSlice(idx_t start_block_index, idx_t end_block_index, idx_t end_entry_index);
	//! Unswizzles all
	void Unswizzle();

public:
	const SortedDataType type;
	//! Layout of this data
	const RowLayout layout;
	//! Data and heap blocks
	vector<RowDataBlock> data_blocks;
	vector<RowDataBlock> heap_blocks;
	//! Whether the pointers in this sorted data are swizzled
	bool swizzled;

private:
	//! The buffer manager
	BufferManager &buffer_manager;
	//! The global state
	GlobalSortState &state;
};

//! Block that holds sorted rows: radix, blob and payload data
struct SortedBlock {
public:
	SortedBlock(BufferManager &buffer_manager, GlobalSortState &gstate);
	//! Number of rows that this object holds
	idx_t Count() const;
	//! Initialize this block to write data to
	void InitializeWrite();
	//! Init new block to write to
	void CreateBlock();
	//! Fill this sorted block by appending the blocks held by a vector of sorted blocks
	void AppendSortedBlocks(vector<unique_ptr<SortedBlock>> &sorted_blocks);
	//! Locate the block and entry index of a row in this block,
	//! given an index between 0 and the total number of rows in this block
	void GlobalToLocalIndex(const idx_t &global_idx, idx_t &local_block_index, idx_t &local_entry_index);
	//! Create a slice that holds the rows between the start and end indices
	unique_ptr<SortedBlock> CreateSlice(const idx_t start, const idx_t end, idx_t &entry_idx);

	//! Size (in bytes) of the heap of this block
	idx_t HeapSize() const;
	//! Total size (in bytes) of this block
	idx_t SizeInBytes() const;

public:
	//! Radix/memcmp sortable data
	vector<RowDataBlock> radix_sorting_data;
	//! Variable sized sorting data
	unique_ptr<SortedData> blob_sorting_data;
	//! Payload data
	unique_ptr<SortedData> payload_data;

private:
	//! Buffer manager, global state, and sorting layout constants
	BufferManager &buffer_manager;
	GlobalSortState &state;
	const SortLayout &sort_layout;
	const RowLayout &payload_layout;
};

//! State used to scan a SortedBlock e.g. during merge sort
struct SBScanState {
public:
	SBScanState(BufferManager &buffer_manager, GlobalSortState &state);

	void PinRadix(idx_t block_idx_to);
	void PinData(SortedData &sd);

	data_ptr_t RadixPtr() const;
	data_ptr_t DataPtr(SortedData &sd) const;
	data_ptr_t HeapPtr(SortedData &sd) const;
	data_ptr_t BaseHeapPtr(SortedData &sd) const;

	idx_t Remaining() const;

	void SetIndices(idx_t block_idx_to, idx_t entry_idx_to);

public:
	BufferManager &buffer_manager;
	const SortLayout &sort_layout;
	GlobalSortState &state;

	SortedBlock *sb;

	idx_t block_idx;
	idx_t entry_idx;

	unique_ptr<BufferHandle> radix_handle = nullptr;

	unique_ptr<BufferHandle> blob_sorting_data_handle = nullptr;
	unique_ptr<BufferHandle> blob_sorting_heap_handle = nullptr;

	unique_ptr<BufferHandle> payload_data_handle = nullptr;
	unique_ptr<BufferHandle> payload_heap_handle = nullptr;
};

//! Used to scan the data into DataChunks after sorting
struct PayloadScanner {
public:
	PayloadScanner(SortedData &sorted_data, GlobalSortState &global_sort_state, bool flush = true);
	explicit PayloadScanner(GlobalSortState &global_sort_state, bool flush = true);

	//! Scan a single block
	PayloadScanner(GlobalSortState &global_sort_state, idx_t block_idx);

	//! The type layout of the payload
	inline const vector<LogicalType> &GetPayloadTypes() const {
		return sorted_data.layout.GetTypes();
	}

	//! The number of rows scanned so far
	inline idx_t Scanned() const {
		return total_scanned;
	}

	//! The number of remaining rows
	inline idx_t Remaining() const {
		return total_count - total_scanned;
	}

	//! Scans the next data chunk from the sorted data
	void Scan(DataChunk &chunk);

private:
	//! The sorted data being scanned
	SortedData &sorted_data;
	//! Read state
	SBScanState read_state;
	//! The total count of sorted_data
	const idx_t total_count;
	//! The global sort state
	GlobalSortState &global_sort_state;
	//! Addresses used to gather from the sorted data
	Vector addresses = Vector(LogicalType::POINTER);
	//! The number of rows scanned so far
	idx_t total_scanned;
	//! Whether to flush the blocks after scanning
	const bool flush;
};

} // namespace duckdb
