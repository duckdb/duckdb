//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/sort_strategy.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/sorting/sort.hpp"

namespace duckdb {

class SortStrategy {
public:
	using Types = vector<LogicalType>;
	using HashGroupPtr = unique_ptr<ColumnDataCollection>;
	using SortedRunPtr = unique_ptr<SortedRun>;

	explicit SortStrategy(const Types &input_types);
	virtual ~SortStrategy() = default;

public:
	//===--------------------------------------------------------------------===//
	// Sink Interface
	//===--------------------------------------------------------------------===//
	virtual unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const = 0;
	virtual unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &client) const = 0;
	virtual SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const = 0;
	virtual SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const = 0;
	virtual SinkFinalizeType Finalize(ClientContext &client, OperatorSinkFinalizeInput &finalize) const = 0;
	virtual ProgressData GetSinkProgress(ClientContext &context, GlobalSinkState &gstate,
	                                     const ProgressData source_progress) const = 0;
	virtual void Synchronize(const GlobalSinkState &source, GlobalSinkState &target) const;

public:
	//===--------------------------------------------------------------------===//
	// Source Interface
	//===--------------------------------------------------------------------===//
	virtual unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context,
	                                                         GlobalSourceState &gstate) const;
	virtual unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context, GlobalSinkState &sink) const = 0;

public:
	//===--------------------------------------------------------------------===//
	// Non-Standard Interface
	//===--------------------------------------------------------------------===//
	virtual void SortColumnData(ExecutionContext &context, hash_t hash_bin, OperatorSinkFinalizeInput &finalize);

	virtual SourceResultType MaterializeColumnData(ExecutionContext &context, idx_t hash_bin,
	                                               OperatorSourceInput &source) const = 0;
	virtual HashGroupPtr GetColumnData(idx_t hash_bin, OperatorSourceInput &source) const = 0;

	virtual SourceResultType MaterializeSortedRun(ExecutionContext &context, idx_t hash_bin,
	                                              OperatorSourceInput &source) const = 0;
	virtual SortedRunPtr GetSortedRun(ClientContext &client, idx_t hash_bin, OperatorSourceInput &source) const = 0;

	// The chunk and row counts of the hash groups.
	struct ChunkRow {
		idx_t chunks = 0;
		idx_t count = 0;
	};
	using ChunkRows = vector<ChunkRow>;
	virtual const ChunkRows &GetHashGroups(GlobalSourceState &global_state) const = 0;

public:
	// OVER(...) (sorting)
	Types payload_types;
};

} // namespace duckdb
