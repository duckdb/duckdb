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

struct ProgressData;
class SortedRun;
class SortedRunMergerLocalState;
class SortedRunMergerGlobalState;
enum class SortKeyType : uint8_t;

class SortedRunMerger {
	friend class SortedRunMergerLocalState;
	friend class SortedRunMergerGlobalState;

public:
	SortedRunMerger(const TupleDataLayout &key_layout, vector<unique_ptr<SortedRun>> &&sorted_runs,
	                const vector<SortProjectionColumn> &output_projection_columns, idx_t partition_size, bool external,
	                bool fixed_blocks);

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
	const TupleDataLayout &key_layout;
	vector<unique_ptr<SortedRun>> sorted_runs;
	const vector<SortProjectionColumn> &output_projection_columns;
	const idx_t total_count;

	const idx_t partition_size;
	const bool external;
	const bool fixed_blocks;
};

} // namespace duckdb
