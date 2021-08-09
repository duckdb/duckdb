//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sort/sort.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sort/sorted_block.hpp"
#include "duckdb/common/types/row_data_collection.hpp"
#include "duckdb/common/types/row_layout.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

struct SortLayout {
public:
	SortLayout(const vector<BoundOrderByNode> &orders, const vector<unique_ptr<BaseStatistics>> &statistics);

public:
	idx_t column_count;
	vector<OrderType> order_types;
	vector<OrderByNullType> order_by_null_types;
	vector<LogicalType> logical_types;

	bool all_constant;
	vector<bool> constant_size;
	vector<idx_t> column_sizes;
	vector<BaseStatistics *> stats;
	vector<bool> has_null;

	idx_t comparison_size;
	idx_t entry_size;

	RowLayout blob_layout;
	unordered_map<idx_t, idx_t> sorting_to_blob_col;
};

struct GlobalSortState {
public:
	GlobalSortState(BufferManager &buffer_manager, vector<BoundOrderByNode> &orders,
	                vector<unique_ptr<BaseStatistics>> &statistics, RowLayout &payload_layout);

	//! Prepares the GlobalSortState for the merge sort phase (after completing radix sort phase)
	void PrepareForMerge();
	//! Initializes the global sort state for another round of merging
	void InitMergeRound();
	//! Completes the cascaded merge sort round
	void CompleteRound();

public:
	//! The lock for updating the order global state
	mutex lock;
	//! The buffer manager
	BufferManager &buffer_manager;

	//! Constants concerning sorting and payload data
	const SortLayout sort_layout;
	const RowLayout payload_layout;

	//! Sorted data
	vector<unique_ptr<SortedBlock>> sorted_blocks;
	vector<vector<unique_ptr<SortedBlock>>> sorted_blocks_temp;
	unique_ptr<SortedBlock> odd_one_out = nullptr;
	//! Pinned heap data (if sorting in memory)
	vector<RowDataBlock> heap_blocks;
	vector<unique_ptr<BufferHandle>> pinned_blocks;

	//! Total row count
	idx_t total_count;
	//! Capacity (number of rows) used to initialize blocks
	idx_t block_capacity;

	//! Whether we are doing an external sort
	bool external;

	//! Progress in merge path stage
	idx_t pair_idx;
	idx_t num_pairs;
	idx_t l_start;
	idx_t r_start;
};

struct LocalSortState {
public:
	LocalSortState();

	//! Initialize the layouts and RowDataCollections
	void Initialize(GlobalSortState &global_sort_state, BufferManager &buffer_manager_p);
	//! Sink one DataChunk into the local sort state
	void SinkChunk(DataChunk &sort, DataChunk &payload);
	//! Size of accumulated data in bytes
	idx_t SizeInBytes() const;
	//! Sort the data accumulated so far
	void Sort(GlobalSortState &global_sort_state);

private:
	RowDataBlock ConcatenateBlocks(RowDataCollection &row_data);
	void ReOrder(SortedData &sd, data_ptr_t sorting_ptr, RowDataCollection &heap, GlobalSortState &gstate);
	void ReOrder(SortedBlock &sb, GlobalSortState &gstate);
	//! Sorts the data in a SortedBlock (static method, single-threaded radix-sort)
	void SortInMemory();

public:
	//! Whether this local state has been initialized
	bool initialized;
	//! The buffer manager
	BufferManager *buffer_manager;
	//! The sorting and payload layouts
	const SortLayout *sort_layout;
	const RowLayout *payload_layout;
	//! Radix/memcmp sortable data
	unique_ptr<RowDataCollection> radix_sorting_data;
	//! Variable sized sorting data and accompanying heap
	unique_ptr<RowDataCollection> blob_sorting_data;
	unique_ptr<RowDataCollection> blob_sorting_heap;
	//! Payload data and accompanying heap
	unique_ptr<RowDataCollection> payload_data;
	unique_ptr<RowDataCollection> payload_heap;
	//! Sorted data
	vector<unique_ptr<SortedBlock>> sorted_blocks;

private:
	//! Constant buffers allocated for vector serialization
	const SelectionVector &sel_ptr = FlatVector::INCREMENTAL_SELECTION_VECTOR;
	Vector addresses = Vector(LogicalType::POINTER);
};

struct MergeSorter {
public:
	MergeSorter(GlobalSortState &state, BufferManager &buffer_manager);

	//! Iterates over the current cascaded merge sort round
	void Iterate();

private:
	//! Sets the left and right block that will be merged next
	void GetNextPartition();
	//! Finds the next partition using Merge path
	void GetIntersection(SortedBlock &l, SortedBlock &r, const idx_t diagonal, idx_t &l_idx, idx_t &r_idx);
	//! Compare values within SortedBlocks using a global index
	int CompareUsingGlobalIndex(SortedBlock &l, SortedBlock &r, const idx_t l_idx, const idx_t r_idx);

	//! Finds the next partition and merges it
	void MergePartition();

	//! Computes how the next 'count' tuples should be merged by setting the 'left_smaller' array
	void ComputeMerge(const idx_t &count, bool left_smaller[]);

	//! Merges the radix sorting blocks according to the 'left_smaller' array
	void MergeRadix(const idx_t &count, const bool left_smaller[]);
	//! Merges SortedData according to the 'left_smaller' array
	void MergeData(SortedData &result_data, SortedData &l_data, SortedData &r_data, const idx_t &count,
	               const bool left_smaller[], idx_t next_entry_sizes[]);
	//! Merges constant size rows according to the 'left_smaller' array
	void MergeRows(data_ptr_t &l_ptr, idx_t &l_entry_idx, const idx_t &l_count, data_ptr_t &r_ptr, idx_t &r_entry_idx,
	               const idx_t &r_count, RowDataBlock *target_block, data_ptr_t &target_ptr, const idx_t &entry_size,
	               const bool left_smaller[], idx_t &copied, const idx_t &count);
	//! Flushes constant size rows into the result
	void FlushRows(data_ptr_t &source_ptr, idx_t &source_entry_idx, const idx_t &source_count,
	               RowDataBlock *target_block, data_ptr_t &target_ptr, const idx_t &entry_size, idx_t &copied,
	               const idx_t &count);
	//! Flushes blob rows and accompanying heap
	void FlushBlobs(const RowLayout &layout, const idx_t &source_count, data_ptr_t &source_data_ptr,
	                idx_t &source_entry_idx, data_ptr_t &source_heap_ptr, RowDataBlock *target_data_block,
	                data_ptr_t &target_data_ptr, RowDataBlock *target_heap_block, BufferHandle &target_heap_handle,
	                data_ptr_t &target_heap_ptr, idx_t &copied, const idx_t &count);

private:
	//! The global sorting state
	GlobalSortState &state;
	//! The sorting and payload layouts
	BufferManager &buffer_manager;
	const SortLayout &sort_layout;

	//! The left, right and result blocks of the current partition
	unique_ptr<SortedBlock> left_block;
	unique_ptr<SortedBlock> right_block;
	SortedBlock *result;
};

} // namespace duckdb
