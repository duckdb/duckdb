//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/hashed_sort.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sorting/sort.hpp"

namespace duckdb {

// Formerly PartitionGlobalHashGroup
class HashedSortGroup {
public:
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;

	HashedSortGroup(ClientContext &client, optional_ptr<Sort> sort, idx_t group_idx);

	const idx_t group_idx;

	//	Sink
	optional_ptr<Sort> sort;
	unique_ptr<GlobalSinkState> sort_global;

	//	Source
	atomic<idx_t> tasks_completed;
	unique_ptr<GlobalSourceState> sort_source;
	unique_ptr<ColumnDataCollection> sorted;
};

class HashedSortCallback {
public:
	virtual ~HashedSortCallback() = default;
	virtual void OnSortedGroup(HashedSortGroup &hash_group) const = 0;
};

class HashedSort {
public:
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;
	using HashGroupPtr = unique_ptr<HashedSortGroup>;

	static void GenerateOrderings(Orders &partitions, Orders &orders,
	                              const vector<unique_ptr<Expression>> &partition_bys, const Orders &order_bys,
	                              const vector<unique_ptr<BaseStatistics>> &partitions_stats);

	HashedSort(ClientContext &context, const vector<unique_ptr<Expression>> &partition_bys,
	           const vector<BoundOrderByNode> &order_bys, const Types &payload_types,
	           const vector<unique_ptr<BaseStatistics>> &partitions_stats, idx_t estimated_cardinality,
	           optional_ptr<HashedSortCallback> callback);

public:
	//===--------------------------------------------------------------------===//
	// Sink Interface
	//===--------------------------------------------------------------------===//
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &client) const;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const;
	SinkFinalizeType Finalize(ClientContext &client, OperatorSinkFinalizeInput &finalize) const;

public:
	//===--------------------------------------------------------------------===//
	// Non-Standard Interface
	//===--------------------------------------------------------------------===//
	SinkFinalizeType MaterializeHashGroups(Pipeline &pipeline, Event &event, const PhysicalOperator &op,
	                                       OperatorSinkFinalizeInput &finalize) const;
	vector<HashGroupPtr> &GetHashGroups(GlobalSinkState &global_state) const;

public:
	ClientContext &client;
	//! The host's estimated row count
	const idx_t estimated_cardinality;

	// OVER(...) (sorting)
	Orders partitions;
	Orders orders;
	idx_t sort_col_count;
	Types payload_types;
	// Input columns in the sorted output
	vector<column_t> scan_ids;
	// Key columns in the sorted output
	vector<column_t> sort_ids;
	// Key columns that must be computed
	vector<unique_ptr<Expression>> sort_exprs;
	//! Common sort description
	unique_ptr<Sort> sort;
	//! Sorting callback for completed groups
	mutable optional_ptr<HashedSortCallback> callback;
};

} // namespace duckdb
