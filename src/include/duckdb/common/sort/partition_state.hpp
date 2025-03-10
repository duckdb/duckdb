//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sort/partition_state.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sort/sort.hpp"
#include "duckdb/common/types/column/partitioned_column_data.hpp"
#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"

namespace duckdb {

class PartitionGlobalHashGroup {
public:
	using GlobalSortStatePtr = unique_ptr<GlobalSortState>;
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;
	using OrderMasks = unordered_map<idx_t, ValidityMask>;

	PartitionGlobalHashGroup(ClientContext &context, const Orders &partitions, const Orders &orders,
	                         const Types &payload_types, bool external);

	inline int ComparePartitions(const SBIterator &left, const SBIterator &right) {
		int part_cmp = 0;
		if (partition_layout.all_constant) {
			part_cmp = FastMemcmp(left.entry_ptr, right.entry_ptr, partition_layout.comparison_size);
		} else {
			part_cmp = Comparators::CompareTuple(left.scan, right.scan, left.entry_ptr, right.entry_ptr,
			                                     partition_layout, left.external);
		}
		return part_cmp;
	}

	void ComputeMasks(ValidityMask &partition_mask, OrderMasks &order_masks);

	GlobalSortStatePtr global_sort;
	atomic<idx_t> count;

	// Mask computation
	SortLayout partition_layout;
};

class PartitionGlobalSinkState {
public:
	using HashGroupPtr = unique_ptr<PartitionGlobalHashGroup>;
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;

	using GroupingPartition = unique_ptr<PartitionedTupleData>;
	using GroupingAppend = unique_ptr<PartitionedTupleDataAppendState>;

	static void GenerateOrderings(Orders &partitions, Orders &orders,
	                              const vector<unique_ptr<Expression>> &partition_bys, const Orders &order_bys,
	                              const vector<unique_ptr<BaseStatistics>> &partitions_stats);

	PartitionGlobalSinkState(ClientContext &context, const vector<unique_ptr<Expression>> &partition_bys,
	                         const vector<BoundOrderByNode> &order_bys, const Types &payload_types,
	                         const vector<unique_ptr<BaseStatistics>> &partitions_stats, idx_t estimated_cardinality);
	virtual ~PartitionGlobalSinkState() = default;

	bool HasMergeTasks() const;

	unique_ptr<RadixPartitionedTupleData> CreatePartition(idx_t new_bits) const;
	void SyncPartitioning(const PartitionGlobalSinkState &other);

	void UpdateLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append);
	void CombineLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append);

	virtual void OnBeginMerge() {};
	virtual void OnSortedPartition(const idx_t hash_bin_p) {};

	ClientContext &context;
	BufferManager &buffer_manager;
	Allocator &allocator;
	mutex lock;

	// OVER(PARTITION BY...) (hash grouping)
	unique_ptr<RadixPartitionedTupleData> grouping_data;
	//! Payload plus hash column
	TupleDataLayout grouping_types;
	//! The number of radix bits if this partition is being synced with another
	idx_t fixed_bits;

	// OVER(...) (sorting)
	Orders partitions;
	Orders orders;
	const Types payload_types;
	vector<HashGroupPtr> hash_groups;
	bool external;
	//	Reverse lookup from hash bins to non-empty hash groups
	vector<size_t> bin_groups;

	// OVER() (no sorting)
	unique_ptr<RowDataCollection> rows;
	unique_ptr<RowDataCollection> strings;

	// Threading
	idx_t memory_per_thread;
	idx_t max_bits;
	atomic<idx_t> count;

private:
	void ResizeGroupingData(idx_t cardinality);
	void SyncLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append);
};

class PartitionLocalSinkState {
public:
	using LocalSortStatePtr = unique_ptr<LocalSortState>;

	PartitionLocalSinkState(ClientContext &context, PartitionGlobalSinkState &gstate_p);

	// Global state
	PartitionGlobalSinkState &gstate;
	Allocator &allocator;

	//	Shared expression evaluation
	ExpressionExecutor executor;
	DataChunk group_chunk;
	DataChunk payload_chunk;
	size_t sort_cols;

	// OVER(PARTITION BY...) (hash grouping)
	unique_ptr<PartitionedTupleData> local_partition;
	unique_ptr<PartitionedTupleDataAppendState> local_append;

	// OVER(ORDER BY...) (only sorting)
	LocalSortStatePtr local_sort;

	// OVER() (no sorting)
	RowLayout payload_layout;
	unique_ptr<RowDataCollection> rows;
	unique_ptr<RowDataCollection> strings;

	//! Compute the hash values
	void Hash(DataChunk &input_chunk, Vector &hash_vector);
	//! Sink an input chunk
	void Sink(DataChunk &input_chunk);
	//! Merge the state into the global state.
	void Combine();
};

enum class PartitionSortStage : uint8_t { INIT, SCAN, PREPARE, MERGE, SORTED, FINISHED };

class PartitionLocalMergeState;

class PartitionGlobalMergeState {
public:
	using GroupDataPtr = unique_ptr<TupleDataCollection>;

	//	OVER(PARTITION BY...)
	PartitionGlobalMergeState(PartitionGlobalSinkState &sink, GroupDataPtr group_data, hash_t hash_bin);

	//	OVER(ORDER BY...)
	explicit PartitionGlobalMergeState(PartitionGlobalSinkState &sink);

	bool IsFinished() const {
		lock_guard<mutex> guard(lock);
		return stage == PartitionSortStage::FINISHED;
	}

	bool AssignTask(PartitionLocalMergeState &local_state);
	bool TryPrepareNextStage();
	void CompleteTask();

	PartitionGlobalSinkState &sink;
	GroupDataPtr group_data;
	PartitionGlobalHashGroup *hash_group;
	const idx_t group_idx;
	vector<column_t> column_ids;
	TupleDataParallelScanState chunk_state;
	GlobalSortState *global_sort;
	const idx_t memory_per_thread;
	const idx_t num_threads;

private:
	mutable mutex lock;
	PartitionSortStage stage;
	idx_t total_tasks;
	idx_t tasks_assigned;
	idx_t tasks_completed;
};

class PartitionLocalMergeState {
public:
	explicit PartitionLocalMergeState(PartitionGlobalSinkState &gstate);

	bool TaskFinished() {
		return finished;
	}

	void Prepare();
	void Scan();
	void Merge();
	void Sorted();

	void ExecuteTask();

	PartitionGlobalMergeState *merge_state;
	PartitionSortStage stage;
	atomic<bool> finished;

	//	Sorting buffers
	ExpressionExecutor executor;
	DataChunk sort_chunk;
	DataChunk payload_chunk;
};

class PartitionGlobalMergeStates {
public:
	struct Callback {
		virtual ~Callback() = default;

		virtual bool HasError() const {
			return false;
		}
	};

	using PartitionGlobalMergeStatePtr = unique_ptr<PartitionGlobalMergeState>;

	explicit PartitionGlobalMergeStates(PartitionGlobalSinkState &sink);

	bool ExecuteTask(PartitionLocalMergeState &local_state, Callback &callback);

	vector<PartitionGlobalMergeStatePtr> states;
};

class PartitionMergeEvent : public BasePipelineEvent {
public:
	PartitionMergeEvent(PartitionGlobalSinkState &gstate_p, Pipeline &pipeline_p, const PhysicalOperator &op_p)
	    : BasePipelineEvent(pipeline_p), gstate(gstate_p), merge_states(gstate_p), op(op_p) {
	}

	PartitionGlobalSinkState &gstate;
	PartitionGlobalMergeStates merge_states;
	const PhysicalOperator &op;

public:
	void Schedule() override;
};

} // namespace duckdb
