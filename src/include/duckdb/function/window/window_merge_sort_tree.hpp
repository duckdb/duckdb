//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_merge_sort_tree.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/merge_sort_tree.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

#include "duckdb/function/window/window_aggregator.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/sort/partition_state.hpp"

namespace duckdb {

class WindowMergeSortTree;

class WindowMergeSortTreeLocalState : public WindowAggregatorState {
public:
	explicit WindowMergeSortTreeLocalState(WindowMergeSortTree &index_tree);

	//! Add a chunk to the local sort
	void SinkChunk(DataChunk &chunk, const idx_t row_idx, optional_ptr<SelectionVector> filter_sel, idx_t filtered);
	//! Sort the data
	void Sort();
	//! Process sorted leaf data
	virtual void BuildLeaves() = 0;

	//! The index tree we are building
	WindowMergeSortTree &window_tree;
	//! Thread-local sorting data
	optional_ptr<LocalSortState> local_sort;
	//! Buffer for the sort keys
	DataChunk sort_chunk;
	//! Buffer for the payload data
	DataChunk payload_chunk;
	//! Build stage
	PartitionSortStage build_stage = PartitionSortStage::INIT;
	//! Build task number
	idx_t build_task;

private:
	void ExecuteSortTask();
};

class WindowMergeSortTree {
public:
	using GlobalSortStatePtr = unique_ptr<GlobalSortState>;
	using LocalSortStatePtr = unique_ptr<LocalSortState>;

	WindowMergeSortTree(ClientContext &context, const vector<BoundOrderByNode> &orders,
	                    const vector<column_t> &sort_idx, const idx_t count, bool unique = false);
	virtual ~WindowMergeSortTree() = default;

	virtual unique_ptr<WindowAggregatorState> GetLocalState() = 0;

	//! Make a local sort for a thread
	optional_ptr<LocalSortState> AddLocalSort();

	//! Thread-safe post-sort cleanup
	virtual void CleanupSort();

	//! Sort state machine
	bool TryPrepareSortStage(WindowMergeSortTreeLocalState &lstate);
	//! Build the MST in parallel from the sorted data
	void Build();

	//! The query context
	ClientContext &context;
	//! Thread memory limit
	const idx_t memory_per_thread;
	//! The column indices for sorting
	const vector<column_t> sort_idx;
	//! The sorted data
	GlobalSortStatePtr global_sort;
	//! Finalize guard
	mutex lock;
	//! Local sort set
	vector<LocalSortStatePtr> local_sorts;
	//! Finalize stage
	atomic<PartitionSortStage> build_stage;
	//! Tasks launched
	idx_t total_tasks = 0;
	//! Tasks launched
	idx_t tasks_assigned = 0;
	//! Tasks landed
	atomic<idx_t> tasks_completed;
	//! The block starts (the scanner doesn't know this) plus the total count
	vector<idx_t> block_starts;

	// Merge sort trees for various sizes
	// Smaller is probably not worth the effort.
	using MergeSortTree32 = MergeSortTree<uint32_t, uint32_t>;
	using MergeSortTree64 = MergeSortTree<uint64_t, uint64_t>;
	unique_ptr<MergeSortTree32> mst32;
	unique_ptr<MergeSortTree64> mst64;

protected:
	//! Find the starts of all the blocks
	//! Returns the total number of rows
	virtual idx_t MeasurePayloadBlocks();
};

} // namespace duckdb
