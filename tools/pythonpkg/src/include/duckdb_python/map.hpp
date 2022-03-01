//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb_python/pandas_scan.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb_python/pybind_wrapper.hpp"
#include "duckdb/parser/parsed_data/create_table_function_info.hpp"

namespace duckdb {

struct MapFunction : public TableFunction {

public:
	MapFunction();

	static unique_ptr<FunctionData> MapFunctionBind(ClientContext &context, vector<Value> &inputs,
	                                                named_parameter_map_t &named_parameters,
	                                                vector<LogicalType> &input_table_types,
	                                                vector<string> &input_table_names,
	                                                vector<LogicalType> &return_types, vector<string> &names);

	static void MapFunctionExec(ClientContext &context, const FunctionData *bind_data,
	                            FunctionOperatorData *operator_state, DataChunk *input, DataChunk &output);
};

} // namespace duckdb
