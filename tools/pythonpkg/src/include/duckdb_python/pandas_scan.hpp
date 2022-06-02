//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pandas_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

#include "duckdb_python/pybind_wrapper.hpp"

namespace duckdb {

struct PandasScanFunction : public TableFunction {
public:
	static constexpr idx_t PANDAS_PARTITION_COUNT = 50 * STANDARD_VECTOR_SIZE;

public:
	PandasScanFunction();

	static unique_ptr<FunctionData> PandasScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                               vector<LogicalType> &return_types, vector<string> &names);

	static unique_ptr<GlobalTableFunctionState> PandasScanInit(ClientContext &context, const FunctionData *bind_data_p,
	                                                           const vector<column_t> &column_ids,
	                                                           TableFilterSet *filters);

	static idx_t PandasScanMaxThreads(ClientContext &context, const FunctionData *bind_data_p);

	static unique_ptr<ParallelState> PandasScanInitParallelState(ClientContext &context,
	                                                             const FunctionData *bind_data_p,
	                                                             const vector<column_t> &column_ids,
	                                                             TableFilterSet *filters);

	static unique_ptr<GlobalTableFunctionState>
	PandasScanParallelInit(ClientContext &context, const FunctionData *bind_data_p, ParallelState *state,
	                       const vector<column_t> &column_ids, TableFilterSet *filters);

	static bool PandasScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
	                                        GlobalTableFunctionState *operator_state, ParallelState *parallel_state_p);

	static double PandasProgress(ClientContext &context, const FunctionData *bind_data_p);

	//! The main pandas scan function: note that this can be called in parallel without the GIL
	//! hence this needs to be GIL-safe, i.e. no methods that create Python objects are allowed
	static void PandasScanFunc(ClientContext &context, const FunctionData *bind_data,
	                           GlobalTableFunctionState *operator_state, DataChunk &output);

	static void PandasScanFuncParallel(ClientContext &context, const FunctionData *bind_data,
	                                   GlobalTableFunctionState *operator_state, DataChunk &output,
	                                   ParallelState *parallel_state_p);

	static unique_ptr<NodeStatistics> PandasScanCardinality(ClientContext &context, const FunctionData *bind_data);

	static idx_t PandasScanGetBatchIndex(ClientContext &context, const FunctionData *bind_data_p,
	                                     GlobalTableFunctionState *operator_state, ParallelState *parallel_state_p);

	// Helper function that transform pandas df names to make them work with our binder
	static py::object PandasReplaceCopiedNames(const py::object &original_df);
};

} // namespace duckdb
