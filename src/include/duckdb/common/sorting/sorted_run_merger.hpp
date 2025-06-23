//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/common/sorting/sorted_run_merger.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/execution/physical_operator_states.hpp"
#include "duckdb/common/sorting/sort_projection_column.hpp"

namespace duckdb {

class TupleDataLayout;
struct BoundOrderByNode;
struct ProgressData;
class SortedRun;
enum class SortKeyType : uint8_t;

class SortedRunMerger {
	friend class SortedRunMergerLocalState;
	friend class SortedRunMergerGlobalState;

public:
	SortedRunMerger(const Expression &decode_sort_key, shared_ptr<TupleDataLayout> key_layout,
	                vector<unique_ptr<SortedRun>> &&sorted_runs,
	                const vector<SortProjectionColumn> &output_projection_columns, idx_t partition_size, bool external,
	                bool is_index_sort);

public:
	//===--------------------------------------------------------------------===//
	// Source Interface
	//===--------------------------------------------------------------------===//
	unique_ptr<LocalSourceState> GetLocalSourceState(ExecutionContext &context, GlobalSourceState &gstate) const;
	unique_ptr<GlobalSourceState> GetGlobalSourceState(ClientContext &context) const;
	SourceResultType GetData(ExecutionContext &context, DataChunk &chunk, OperatorSourceInput &input) const;
	OperatorPartitionData GetPartitionData(ExecutionContext &context, DataChunk &chunk, GlobalSourceState &gstate,
	                                       LocalSourceState &lstate, const OperatorPartitionInfo &partition_info) const;
	ProgressData GetProgress(ClientContext &context, GlobalSourceState &gstate) const;

public:
	const Expression &decode_sort_key;
	shared_ptr<TupleDataLayout> key_layout;
	vector<unique_ptr<SortedRun>> sorted_runs;
	const vector<SortProjectionColumn> &output_projection_columns;
	const idx_t total_count;

	const idx_t partition_size;
	const bool external;
	const bool is_index_sort;
};

} // namespace duckdb
