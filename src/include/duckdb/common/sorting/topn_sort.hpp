//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/topn_sort.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sorting/sort_strategy.hpp"

namespace duckdb {

class TopNSort : public SortStrategy {
public:
	using Orders = vector<BoundOrderByNode>;

	static bool CanOptimize(optional_idx limit, idx_t cardinality);

	TopNSort(ClientContext &client, const vector<BoundOrderByNode> &order_bys, const Types &payload_types,
	         optional_idx partition_limit);

public:
	//===--------------------------------------------------------------------===//
	// Sink Interface
	//===--------------------------------------------------------------------===//
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const override;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &client) const override;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const override;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const override;
	SinkFinalizeType Finalize(ClientContext &client, OperatorSinkFinalizeInput &finalize) const override;
	ProgressData GetSinkProgress(ClientContext &context, GlobalSinkState &gstate,
	                             const ProgressData source_progress) const override;

public:
	//===--------------------------------------------------------------------===//
	// Source Interface
	//===--------------------------------------------------------------------===//
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context, GlobalSinkState &sink) const override;

public:
	//===--------------------------------------------------------------------===//
	// Non-Standard Interface
	//===--------------------------------------------------------------------===//
	SourceResultType MaterializeColumnData(ExecutionContext &context, idx_t hash_bin,
	                                       OperatorSourceInput &source) const override;
	HashGroupPtr GetColumnData(idx_t hash_bin, OperatorSourceInput &source) const override;

	SourceResultType MaterializeSortedRun(ExecutionContext &context, idx_t hash_bin,
	                                      OperatorSourceInput &source) const override;
	SortedRunPtr GetSortedRun(ClientContext &client, idx_t hash_bin, OperatorSourceInput &source) const override;

	const ChunkRows &GetHashGroups(GlobalSourceState &global_state) const override;

public:
	// OVER(...) (sorting)
	Orders orders;
	// Key columns that must be computed
	vector<unique_ptr<Expression>> sort_exprs;
	//! NOT DISTINCT FROM comparison of the boundary value for every order column
	vector<unique_ptr<Expression>> not_distinct_from;
};

} // namespace duckdb
