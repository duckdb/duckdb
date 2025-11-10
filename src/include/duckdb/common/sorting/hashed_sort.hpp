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

class HashedSort {
public:
	using Orders = vector<BoundOrderByNode>;
	using Types = vector<LogicalType>;
	using HashGroupPtr = unique_ptr<ColumnDataCollection>;
	using SortedRunPtr = unique_ptr<SortedRun>;

	static void GenerateOrderings(Orders &partitions, Orders &orders,
	                              const vector<unique_ptr<Expression>> &partition_bys, const Orders &order_bys,
	                              const vector<unique_ptr<BaseStatistics>> &partitions_stats);

	HashedSort(ClientContext &context, const vector<unique_ptr<Expression>> &partition_bys,
	           const vector<BoundOrderByNode> &order_bys, const Types &payload_types,
	           const vector<unique_ptr<BaseStatistics>> &partitions_stats, idx_t estimated_cardinality,
	           bool require_payload = false);

public:
	//===--------------------------------------------------------------------===//
	// Sink Interface
	//===--------------------------------------------------------------------===//
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &client) const;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const;
	SinkFinalizeType Finalize(ClientContext &client, OperatorSinkFinalizeInput &finalize) const;
	ProgressData GetSinkProgress(ClientContext &context, GlobalSinkState &gstate,
	                             const ProgressData source_progress) const;
	void Synchronize(const GlobalSinkState &source, GlobalSinkState &target) const;

public:
	//===--------------------------------------------------------------------===//
	// Source Interface
	//===--------------------------------------------------------------------===//
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context, GlobalSinkState &sink) const;

public:
	//===--------------------------------------------------------------------===//
	// Non-Standard Interface
	//===--------------------------------------------------------------------===//
	void SortColumnData(ExecutionContext &context, hash_t hash_bin, OperatorSinkFinalizeInput &finalize);

	SourceResultType MaterializeColumnData(ExecutionContext &context, idx_t hash_bin,
	                                       OperatorSourceInput &source) const;
	HashGroupPtr GetColumnData(idx_t hash_bin, OperatorSourceInput &source) const;

	SourceResultType MaterializeSortedRun(ExecutionContext &context, idx_t hash_bin, OperatorSourceInput &source) const;
	SortedRunPtr GetSortedRun(ClientContext &client, idx_t hash_bin, OperatorSourceInput &source) const;

	// The chunk and row counts of the hash groups.
	struct ChunkRow {
		idx_t chunks = 0;
		idx_t count = 0;
	};
	using ChunkRows = vector<ChunkRow>;
	const ChunkRows &GetHashGroups(GlobalSourceState &global_state) const;

public:
	ClientContext &client;
	//! The host's estimated row count
	const idx_t estimated_cardinality;

	// OVER(...) (sorting)
	Orders partitions;
	Orders orders;
	idx_t sort_col_count;
	Types payload_types;
	//! Are we creating a dummy payload column?
	bool force_payload = false;
	// Input columns in the sorted output
	vector<column_t> scan_ids;
	// Key columns in the sorted output
	vector<column_t> sort_ids;
	// Key columns that must be computed
	vector<unique_ptr<Expression>> sort_exprs;
	//! Common sort description
	unique_ptr<Sort> sort;
};

} // namespace duckdb
