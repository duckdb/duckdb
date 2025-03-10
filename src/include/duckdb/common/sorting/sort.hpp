//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/sort.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator.hpp"
#include "duckdb/common/types/row/tuple_data_collection.hpp"

namespace duckdb {

class Sort {
private:
	struct SortProjectionColumn {
		bool is_payload;
		idx_t layout_col_idx;
		idx_t output_col_idx;
	};

public:
	Sort(const vector<BoundOrderByNode> &orders, const vector<LogicalType> &types, vector<idx_t> projection_map);

private:
	//! Key expressions and layout of key columns
	vector<unique_ptr<Expression>> key_expressions;
	TupleDataLayout key_layout;

	//! Projection map and payload layout (columns that also appear as key eliminated)
	vector<idx_t> payload_projection_map;
	TupleDataLayout payload_layout;

	//! Mapping from key/payload layouts to output columns
	vector<idx_t> input_projection_map;
	vector<SortProjectionColumn> output_projection_columns;

public:
	//===--------------------------------------------------------------------===//
	// Sink Interface
	//===--------------------------------------------------------------------===//
	unique_ptr<LocalSinkState> GetLocalSinkState(ExecutionContext &context) const;
	unique_ptr<GlobalSinkState> GetGlobalSinkState(ClientContext &context) const;
	SinkResultType Sink(ExecutionContext &context, DataChunk &chunk, OperatorSinkInput &input) const;
	SinkCombineResultType Combine(ExecutionContext &context, OperatorSinkCombineInput &input) const;
	SinkFinalizeType Finalize(Pipeline &pipeline, Event &event, ClientContext &context,
	                          OperatorSinkFinalizeInput &input) const;
	ProgressData GetSinkProgress(ClientContext &context, GlobalSinkState &gstate,
	                             const ProgressData source_progress) const;

public:
	//===--------------------------------------------------------------------===//
	// Source Interface
	//===--------------------------------------------------------------------===//
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context, GlobalSinkState &sink) const;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const;
	OperatorPartitionData GetPartitionData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	                                       LocalSourceState &lstate, const OperatorPartitionInfo &partition_info) const;
	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate) const;
};

} // namespace duckdb
