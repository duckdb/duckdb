//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/hashed_sort.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/radix_partitioning.hpp"
#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"

namespace duckdb {

// Formerly PartitionGlobalHashGroup
class HashedSortGroup {
public:
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;
	using OrderMasks = unordered_map<idx_t, ValidityMask>;

	HashedSortGroup(ClientContext &context, const Orders &orders, const Types &input_types, idx_t group_idx);

	const idx_t group_idx;

	//	Sink
	unique_ptr<Sort> sort;
	unique_ptr<GlobalSinkState> sort_global;

	//	Source
	atomic<idx_t> tasks_completed;
	unique_ptr<GlobalSourceState> sort_source;
	unique_ptr<ColumnDataCollection> sorted;
};

// Formerly PartitionGlobalSinkState
class HashedSortGlobalSinkState {
public:
	using HashGroupPtr = unique_ptr<HashedSortGroup>;
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;

	using GroupingPartition = unique_ptr<RadixPartitionedTupleData>;
	using GroupingAppend = unique_ptr<PartitionedTupleDataAppendState>;

	static void GenerateOrderings(Orders &partitions, Orders &orders,
	                              const vector<unique_ptr<Expression>> &partition_bys, const Orders &order_bys,
	                              const vector<unique_ptr<BaseStatistics>> &partitions_stats);

	HashedSortGlobalSinkState(ClientContext &context, const vector<unique_ptr<Expression>> &partition_bys,
	                          const vector<BoundOrderByNode> &order_bys, const Types &payload_types,
	                          const vector<unique_ptr<BaseStatistics>> &partitions_stats, idx_t estimated_cardinality);

	bool HasMergeTasks() const;

	// OVER(PARTITION BY...) (hash grouping)
	unique_ptr<RadixPartitionedTupleData> CreatePartition(idx_t new_bits) const;
	void UpdateLocalPartition(GroupingPartition &local_partition, GroupingAppend &partition_append);
	void CombineLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append);
	void Finalize(ClientContext &context, InterruptState &interrupt_state);

	//! System and query state
	ClientContext &context;
	BufferManager &buffer_manager;
	Allocator &allocator;
	mutex lock;

	// OVER(PARTITION BY...) (hash grouping)
	GroupingPartition grouping_data;
	//! Payload plus hash column
	shared_ptr<TupleDataLayout> grouping_types_ptr;
	//! The number of radix bits if this partition is being synced with another
	idx_t fixed_bits;

	// OVER(...) (sorting)
	Orders partitions;
	Orders orders;
	Types payload_types;
	vector<HashGroupPtr> hash_groups;
	// Input columns in the sorted output
	vector<column_t> scan_ids;
	// Key columns in the sorted output
	vector<column_t> sort_ids;
	// Key columns that must be computed
	vector<unique_ptr<Expression>> sort_exprs;

	// OVER() (no sorting)
	unique_ptr<ColumnDataCollection> unsorted;

	// Threading
	idx_t max_bits;
	atomic<idx_t> count;

private:
	void Rehash(idx_t cardinality);
	void SyncLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append);
};

// Formerly PartitionLocalSinkState
class HashedSortLocalSinkState {
public:
	using LocalSortStatePtr = unique_ptr<LocalSinkState>;
	using GroupingPartition = unique_ptr<RadixPartitionedTupleData>;
	using GroupingAppend = unique_ptr<PartitionedTupleDataAppendState>;

	HashedSortLocalSinkState(ExecutionContext &context, HashedSortGlobalSinkState &gstate);

	//! Global state
	HashedSortGlobalSinkState &gstate;
	Allocator &allocator;

	//! Shared expression evaluation
	ExpressionExecutor hash_exec;
	ExpressionExecutor sort_exec;
	DataChunk group_chunk;
	DataChunk sort_chunk;
	DataChunk payload_chunk;
	size_t sort_col_count;

	//! Compute the hash values
	void Hash(DataChunk &input_chunk, Vector &hash_vector);
	//! Sink an input chunk
	void Sink(ExecutionContext &context, DataChunk &input_chunk);
	//! Merge the state into the global state.
	void Combine(ExecutionContext &context);

	// OVER(PARTITION BY...) (hash grouping)
	GroupingPartition local_grouping;
	GroupingAppend grouping_append;

	// OVER(ORDER BY...) (only sorting)
	LocalSortStatePtr sort_local;
	InterruptState interrupt;

	// OVER() (no sorting)
	unique_ptr<ColumnDataCollection> unsorted;
	ColumnDataAppendState unsorted_append;
};

class HashedSortCallback {
public:
	virtual ~HashedSortCallback() = default;
	virtual void OnSortedGroup(HashedSortGroup &hash_group) = 0;
};

// Formerly PartitionMergeEvent
class HashedSortMaterializeEvent : public BasePipelineEvent {
public:
	HashedSortMaterializeEvent(HashedSortGlobalSinkState &gstate, Pipeline &pipeline, const PhysicalOperator &op,
	                           HashedSortCallback *callback);

	HashedSortGlobalSinkState &gstate;
	const PhysicalOperator &op;
	optional_ptr<HashedSortCallback> callback;

public:
	void Schedule() override;
};

} // namespace duckdb
