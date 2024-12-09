#include "duckdb/execution/merge_sort_tree.hpp"
#include "duckdb/planner/bound_result_modifier.hpp"

#include "duckdb/function/window/window_aggregator.hpp"
#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/sort/partition_state.hpp"

namespace duckdb {

class WindowIndexTree;

class WindowIndexTreeLocalState : public WindowAggregatorState {
public:
	explicit WindowIndexTreeLocalState(WindowIndexTree &index_tree);

	//! Add a chunk to the local sort
	void SinkChunk(DataChunk &chunk, const idx_t row_idx, optional_ptr<SelectionVector> filter_sel, idx_t filtered);
	//! Build the MST from the sorted data
	void Build();
	//! Process sorted leaf data
	void BuildLeaves();

	//! The index tree we are building
	WindowIndexTree &index_tree;
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

class WindowIndexTree {
public:
	using GlobalSortStatePtr = unique_ptr<GlobalSortState>;
	using LocalSortStatePtr = unique_ptr<LocalSortState>;

	WindowIndexTree(ClientContext &context, const BoundOrderModifier &orders, vector<column_t> sort_idx,
	                const idx_t count);
	virtual ~WindowIndexTree() = default;

	unique_ptr<WindowAggregatorState> GetLocalState() const;

	//! Make a local sort for a thread
	optional_ptr<LocalSortState> AddLocalSort();

	//! Sort state machine
	bool TryPrepareSortStage(WindowIndexTreeLocalState &lstate);
	//! Thread-safe post-sort cleanup
	void CleanupSort();

	//! Find the Nth index in the set of subframes
	idx_t SelectNth(const SubFrames &frames, idx_t n) const;

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
	idx_t tasks_assigned;
	//! Tasks landed
	atomic<idx_t> tasks_completed;

	// Merge sort trees for various sizes
	// Smaller is probably not worth the effort.
	using MergeSortTree32 = MergeSortTree<uint32_t, uint32_t>;
	using MergeSortTree64 = MergeSortTree<uint64_t, uint64_t>;
	unique_ptr<MergeSortTree32> mst32;
	unique_ptr<MergeSortTree64> mst64;

private:
	//! Find the starts of all the blocks
	void MeasurePayloadBlocks();
	//! The block starts (the scanner doesn't know this) plus the total count
	vector<idx_t> block_starts;
};

} // namespace duckdb
