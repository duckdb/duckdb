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

namespace duckdb {

struct PandasScanFunction : public TableFunction {
public:
	static constexpr idx_t PANDAS_PARTITION_COUNT = 50 * STANDARD_VECTOR_SIZE;

public:
	PandasScanFunction();

	static unique_ptr<FunctionData> PandasScanBind(ClientContext &context, TableFunctionBindInput &input,
	                                               vector<LogicalType> &return_types, vector<string> &names);

	static unique_ptr<FunctionOperatorData> PandasScanInit(ClientContext &context, const FunctionData *bind_data_p,
	                                                       const vector<column_t> &column_ids,
	                                                       TableFilterCollection *filters);

	static idx_t PandasScanMaxThreads(ClientContext &context, const FunctionData *bind_data_p);

	static unique_ptr<ParallelState> PandasScanInitParallelState(ClientContext &context,
	                                                             const FunctionData *bind_data_p,
	                                                             const vector<column_t> &column_ids,
	                                                             TableFilterCollection *filters);

	static unique_ptr<FunctionOperatorData>
	PandasScanParallelInit(ClientContext &context, const FunctionData *bind_data_p, ParallelState *state,
	                       const vector<column_t> &column_ids, TableFilterCollection *filters);

	static bool PandasScanParallelStateNext(ClientContext &context, const FunctionData *bind_data_p,
	                                        FunctionOperatorData *operator_state, ParallelState *parallel_state_p);

	static double PandasProgress(ClientContext &context, const FunctionData *bind_data_p);

	//! The main pandas scan function: note that this can be called in parallel without the GIL
	//! hence this needs to be GIL-safe, i.e. no methods that create Python objects are allowed
	static void PandasScanFunc(ClientContext &context, const FunctionData *bind_data,
	                           FunctionOperatorData *operator_state, DataChunk &output);

	static void PandasScanFuncParallel(ClientContext &context, const FunctionData *bind_data,
	                                   FunctionOperatorData *operator_state, DataChunk &output,
	                                   ParallelState *parallel_state_p);

	static unique_ptr<NodeStatistics> PandasScanCardinality(ClientContext &context, const FunctionData *bind_data);
};

} // namespace duckdb
