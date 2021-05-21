//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/arrow.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/parallel/parallel_state.hpp"
#include "duckdb/common/arrow_duckdb.hpp"
#include "duckdb/common/atomic.hpp"

namespace duckdb {

struct ArrowScanFunctionData : public TableFunctionData {
	ArrowScanFunctionData() : lines_read(0) {
	}
	unique_ptr<ArrowArrayStreamWrapper> stream;
	std::atomic<idx_t> lines_read;
	ArrowSchemaWrapper schema_root;
};

struct ArrowScanState : public FunctionOperatorData {
	explicit ArrowScanState(unique_ptr<ArrowArrayWrapper> current_chunk) : chunk(move(current_chunk)) {
	}
	unique_ptr<ArrowArrayWrapper> chunk;
	idx_t chunk_offset = 0;
	idx_t chunk_idx = 0;
	vector<column_t> column_ids;
};

struct ParallelArrowScanState : public ParallelState {
	ParallelArrowScanState() {
	}
	bool finished = false;
};

struct ArrowTableFunction {
public:
	static void RegisterFunction(BuiltinFunctions &set);

private:
	//! Binds an arrow table
	static unique_ptr<FunctionData> ArrowScanBind(ClientContext &context, vector<Value> &inputs,
	                                              unordered_map<string, Value> &named_parameters,
	                                              vector<LogicalType> &input_table_types,
	                                              vector<string> &input_table_names, vector<LogicalType> &return_types,
	                                              vector<string> &names);
	//! Actual conversion from Arrow to DuckDB
	static void ArrowToDuckDB(ArrowScanState &scan_state, DataChunk &output);

	//! -----Single Thread Functions:-----
	//! Initialize Single Thread Scan
	static unique_ptr<FunctionOperatorData> ArrowScanInit(ClientContext &context, const FunctionData *bind_data,
	                                                      const vector<column_t> &column_ids,
	                                                      TableFilterCollection *filters);

	//! Scan Function for Single Thread Execution
	static void ArrowScanFunction(ClientContext &context, const FunctionData *bind_data,
	                              FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output);

	//! -----Multi Thread Functions:-----
	//! Initialize Parallel State
	static unique_ptr<ParallelState> ArrowScanInitParallelState(ClientContext &context,
	                                                            const FunctionData *bind_data_p);
	//! Initialize Parallel Scans
	static unique_ptr<FunctionOperatorData> ArrowScanParallelInit(ClientContext &context,
	                                                              const FunctionData *bind_data_p, ParallelState *state,
	                                                              const vector<column_t> &column_ids,
	                                                              TableFilterCollection *filters);
	//! Defines Maximum Number of Threads
	static idx_t ArrowScanMaxThreads(ClientContext &context, const FunctionData *bind_data_p);
	//! Scan Function for Parallel Execution
	static void ArrowScanFunctionParallel(ClientContext &context, const FunctionData *bind_data,
	                                      FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output,
	                                      ParallelState *parallel_state_p);
	//! Get next chunk for the running thread
	static bool ArrowScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
	                                       FunctionOperatorData *operator_state, ParallelState *parallel_state_p);

	//! -----Utility Functions:-----
	//! Gets Arrow Table's Cardinality
	static unique_ptr<NodeStatistics> ArrowScanCardinality(ClientContext &context, const FunctionData *bind_data);
	//! Gets the progress on the table scan, used for Progress Bars
	static int ArrowProgress(ClientContext &context, const FunctionData *bind_data_p);
};

} // namespace duckdb
