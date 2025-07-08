//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/hashed_sort.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sorting/sort.hpp"
#include "duckdb/parallel/base_pipeline_event.hpp"

namespace duckdb {

class RadixPartitionedTupleData;
class PartitionedTupleDataAppendState;

// Formerly PartitionGlobalHashGroup
class HashedSortGroup {
public:
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;
	using OrderMasks = unordered_map<idx_t, ValidityMask>;

	HashedSortGroup(ClientContext &context, const Orders &partitions, const Orders &orders, const Types &payload_types,
	                bool external);

	void ComputeMasks(ValidityMask &partition_mask, OrderMasks &order_masks);

	//	Sink
	unique_ptr<Sort> sort;
	unique_ptr<GlobalSinkState> sort_global;
	atomic<idx_t> count;

	//	Source
	atomic<idx_t> tasks_completed;
	unique_ptr<GlobalSourceState> sort_source;
	unique_ptr<ColumnDataCollection> rows;
};

// Formerly PartitionGlobalSinkState
class HashedSortGroupGlobalSinkState {
public:
	using HashGroupPtr = unique_ptr<HashedSortGroup>;
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;

	using GroupingPartition = unique_ptr<RadixPartitionedTupleData>;
	using GroupingAppend = unique_ptr<PartitionedTupleDataAppendState>;

	static void GenerateOrderings(Orders &partitions, Orders &orders,
	                              const vector<unique_ptr<Expression>> &partition_bys, const Orders &order_bys,
	                              const vector<unique_ptr<BaseStatistics>> &partitions_stats);

	HashedSortGroupGlobalSinkState(ClientContext &context, const vector<unique_ptr<Expression>> &partition_bys,
	                               const vector<BoundOrderByNode> &order_bys, const Types &payload_types,
	                               const vector<unique_ptr<BaseStatistics>> &partitions_stats,
	                               idx_t estimated_cardinality);
	virtual ~HashedSortGroupGlobalSinkState() = default;

	bool HasMergeTasks() const;

	// OVER(PARTITION BY...) (hash grouping)
	unique_ptr<RadixPartitionedTupleData> CreatePartition(idx_t new_bits) const;
	void UpdateLocalPartition(GroupingPartition &local_partition, GroupingAppend &partition_append);
	void CombineLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append);

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
	const Types payload_types;
	vector<HashGroupPtr> hash_groups;
	bool external;
	//	Reverse lookup from hash bins to non-empty hash groups
	vector<size_t> bin_groups;

	// OVER() (no sorting)
	unique_ptr<ColumnDataCollection> rows;

	// Threading
	idx_t memory_per_thread;
	idx_t max_bits;
	atomic<idx_t> count;

private:
	void Rehash(idx_t cardinality);
	void SyncLocalPartition(GroupingPartition &local_partition, GroupingAppend &local_append);
};

// Formerly PartitionLocalSinkState
class HashedSortGroupLocalSinkState {
public:
	using LocalSortStatePtr = unique_ptr<LocalSinkState>;
	using GroupingPartition = unique_ptr<RadixPartitionedTupleData>;
	using GroupingAppend = unique_ptr<PartitionedTupleDataAppendState>;

	HashedSortGroupLocalSinkState(ExecutionContext &context, HashedSortGroupGlobalSinkState &gstate);

	//! Global state
	HashedSortGroupGlobalSinkState &gstate;
	ExecutionContext &context;
	Allocator &allocator;

	//! Shared expression evaluation
	ExpressionExecutor executor;
	DataChunk group_chunk;
	DataChunk payload_chunk;
	size_t sort_col_count;

	//! Compute the hash values
	void Hash(DataChunk &input_chunk, Vector &hash_vector);
	//! Sink an input chunk
	void Sink(DataChunk &input_chunk);
	//! Merge the state into the global state.
	void Combine();

	// OVER(PARTITION BY...) (hash grouping)
	GroupingPartition local_grouping;
	GroupingAppend grouping_append;

	// OVER(ORDER BY...) (only sorting)
	LocalSortStatePtr sort_local;
	InterruptState interrupt;

	// OVER() (no sorting)
	TupleDataLayout payload_layout;
	unique_ptr<ColumnDataCollection> rows;
	ColumnDataAppendState rows_append;
};

// Formerly PartitionMergeEvent
class HashedSortMaterializeEvent : public BasePipelineEvent {
public:
	HashedSortMaterializeEvent(HashedSortGroupGlobalSinkState &gstate, Pipeline &pipeline, const PhysicalOperator &op);

	HashedSortGroupGlobalSinkState &gstate;
	const PhysicalOperator &op;

public:
	void Schedule() override;
};

} // namespace duckdb
