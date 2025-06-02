//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sort/sort.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sort/sorted_block.hpp"
#include "duckdb/common/types/row/row_data_collection.hpp"
#include "duckdb/planner/bound_query_node.hpp"

namespace duckdb {

class RowLayout;
struct LocalSortState;

struct SortConstants {
	static constexpr idx_t VALUES_PER_RADIX = 256;
	static constexpr idx_t MSD_RADIX_LOCATIONS = VALUES_PER_RADIX + 1;
	static constexpr idx_t INSERTION_SORT_THRESHOLD = 24;
	static constexpr idx_t MSD_RADIX_SORT_SIZE_THRESHOLD = 4;
};

struct SortLayout {
public:
	SortLayout() {
	}
	explicit SortLayout(const vector<BoundOrderByNode> &orders);
	SortLayout GetPrefixComparisonLayout(idx_t num_prefix_cols) const;

public:
	idx_t column_count;
	vector<OrderType> order_types;
	vector<OrderByNullType> order_by_null_types;
	vector<LogicalType> logical_types;

	bool all_constant;
	vector<bool> constant_size;
	vector<idx_t> column_sizes;
	vector<idx_t> prefix_lengths;
	vector<BaseStatistics *> stats;
	vector<bool> has_null;

	idx_t comparison_size;
	idx_t entry_size;

	RowLayout blob_layout;
	unordered_map<idx_t, idx_t> sorting_to_blob_col;
};

struct GlobalSortState {
public:
	GlobalSortState(ClientContext &context, const vector<BoundOrderByNode> &orders, RowLayout &payload_layout);

	//! Add local state sorted data to this global state
	void AddLocalState(LocalSortState &local_sort_state);
	//! Prepares the GlobalSortState for the merge sort phase (after completing radix sort phase)
	void PrepareMergePhase();
	//! Initializes the global sort state for another round of merging
	void InitializeMergeRound();
	//! Completes the cascaded merge sort round.
	//! Pass true if you wish to use the radix data for further comparisons.
	void CompleteMergeRound(bool keep_radix_data = false);
	//! Print the sorted data to the console.
	void Print();

public:
	//! The client context
	ClientContext &context;
	//! The lock for updating the order global state
	mutex lock;
	//! The buffer manager
	BufferManager &buffer_manager;

	//! Sorting and payload layouts
	const SortLayout sort_layout;
	const RowLayout payload_layout;

	//! Sorted data
	vector<unique_ptr<SortedBlock>> sorted_blocks;
	vector<vector<unique_ptr<SortedBlock>>> sorted_blocks_temp;
	unique_ptr<SortedBlock> odd_one_out;

	//! Pinned heap data (if sorting in memory)
	vector<unique_ptr<RowDataBlock>> heap_blocks;
	vector<BufferHandle> pinned_blocks;

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
	void Sort(GlobalSortState &global_sort_state, bool reorder_heap);
	//! Concatenate the blocks held by a RowDataCollection into a single block
	static unique_ptr<RowDataBlock> ConcatenateBlocks(RowDataCollection &row_data);

private:
	//! Sorts the data in the newly created SortedBlock
	void SortInMemory();
	//! Re-order the local state after sorting
	void ReOrder(GlobalSortState &gstate, bool reorder_heap);
	//! Re-order a SortedData object after sorting
	void ReOrder(SortedData &sd, data_ptr_t sorting_ptr, RowDataCollection &heap, GlobalSortState &gstate,
	             bool reorder_heap);

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
	//! Selection vector and addresses for scattering the data to rows
	const SelectionVector &sel_ptr = *FlatVector::IncrementalSelectionVector();
	Vector addresses = Vector(LogicalType::POINTER);
};

struct MergeSorter {
public:
	MergeSorter(GlobalSortState &state, BufferManager &buffer_manager);

	//! Finds and merges partitions until the current cascaded merge round is finished
	void PerformInMergeRound();

private:
	//! The global sorting state
	GlobalSortState &state;
	//! The sorting and payload layouts
	BufferManager &buffer_manager;
	const SortLayout &sort_layout;

	//! The left and right reader
	unique_ptr<SBScanState> left;
	unique_ptr<SBScanState> right;

	//! Input and output blocks
	unique_ptr<SortedBlock> left_input;
	unique_ptr<SortedBlock> right_input;
	SortedBlock *result;

private:
	//! Computes the left and right block that will be merged next (Merge Path partition)
	void GetNextPartition();
	//! Finds the boundary of the next partition using binary search
	void GetIntersection(const idx_t diagonal, idx_t &l_idx, idx_t &r_idx);
	//! Compare values within SortedBlocks using a global index
	int CompareUsingGlobalIndex(SBScanState &l, SBScanState &r, const idx_t l_idx, const idx_t r_idx);

	//! Finds the next partition and merges it
	void MergePartition();

	//! Computes how the next 'count' tuples should be merged by setting the 'left_smaller' array
	void ComputeMerge(const idx_t &count, bool left_smaller[]);

	//! Merges the radix sorting blocks according to the 'left_smaller' array
	void MergeRadix(const idx_t &count, const bool left_smaller[]);
	//! Merges SortedData according to the 'left_smaller' array
	void MergeData(SortedData &result_data, SortedData &l_data, SortedData &r_data, const idx_t &count,
	               const bool left_smaller[], idx_t next_entry_sizes[], bool reset_indices);
	//! Merges constant size rows according to the 'left_smaller' array
	void MergeRows(data_ptr_t &l_ptr, idx_t &l_entry_idx, const idx_t &l_count, data_ptr_t &r_ptr, idx_t &r_entry_idx,
	               const idx_t &r_count, RowDataBlock &target_block, data_ptr_t &target_ptr, const idx_t &entry_size,
	               const bool left_smaller[], idx_t &copied, const idx_t &count);
	//! Flushes constant size rows into the result
	void FlushRows(data_ptr_t &source_ptr, idx_t &source_entry_idx, const idx_t &source_count,
	               RowDataBlock &target_block, data_ptr_t &target_ptr, const idx_t &entry_size, idx_t &copied,
	               const idx_t &count);
	//! Flushes blob rows and accompanying heap
	void FlushBlobs(const RowLayout &layout, const idx_t &source_count, data_ptr_t &source_data_ptr,
	                idx_t &source_entry_idx, data_ptr_t &source_heap_ptr, RowDataBlock &target_data_block,
	                data_ptr_t &target_data_ptr, RowDataBlock &target_heap_block, BufferHandle &target_heap_handle,
	                data_ptr_t &target_heap_ptr, idx_t &copied, const idx_t &count);
};

struct SBIterator {
	static int ComparisonValue(ExpressionType comparison);

	SBIterator(GlobalSortState &gss, ExpressionType comparison, idx_t entry_idx_p = 0);

	inline idx_t GetIndex() const {
		return entry_idx;
	}

	inline void SetIndex(idx_t entry_idx_p) {
		const auto new_block_idx = entry_idx_p / block_capacity;
		if (new_block_idx != scan.block_idx) {
			scan.SetIndices(new_block_idx, 0);
			if (new_block_idx < block_count) {
				scan.PinRadix(scan.block_idx);
				block_ptr = scan.RadixPtr();
				if (!all_constant) {
					scan.PinData(*scan.sb->blob_sorting_data);
				}
			}
		}

		scan.entry_idx = entry_idx_p % block_capacity;
		entry_ptr = block_ptr + scan.entry_idx * entry_size;
		entry_idx = entry_idx_p;
	}

	inline SBIterator &operator++() {
		if (++scan.entry_idx < block_capacity) {
			entry_ptr += entry_size;
			++entry_idx;
		} else {
			SetIndex(entry_idx + 1);
		}

		return *this;
	}

	inline SBIterator &operator--() {
		if (scan.entry_idx) {
			--scan.entry_idx;
			--entry_idx;
			entry_ptr -= entry_size;
		} else {
			SetIndex(entry_idx - 1);
		}

		return *this;
	}

	inline bool Compare(const SBIterator &other, const SortLayout &prefix) const {
		int comp_res;
		if (all_constant) {
			comp_res = FastMemcmp(entry_ptr, other.entry_ptr, prefix.comparison_size);
		} else {
			comp_res = Comparators::CompareTuple(scan, other.scan, entry_ptr, other.entry_ptr, prefix, external);
		}

		return comp_res <= cmp;
	}

	inline bool Compare(const SBIterator &other) const {
		return Compare(other, sort_layout);
	}

	// Fixed comparison parameters
	const SortLayout &sort_layout;
	const idx_t block_count;
	const idx_t block_capacity;
	const size_t entry_size;
	const bool all_constant;
	const bool external;
	const int cmp;

	// Iteration state
	SBScanState scan;
	idx_t entry_idx;
	data_ptr_t block_ptr;
	data_ptr_t entry_ptr;
};

} // namespace duckdb
