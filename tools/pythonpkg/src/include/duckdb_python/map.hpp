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

	static unique_ptr<FunctionData> MapFunctionBind(ClientContext &context, TableFunctionBindInput &input,
	                                                vector<LogicalType> &return_types, vector<string> &names);

	static OperatorResultType MapFunctionExec(ClientContext &context, TableFunctionInput &data, DataChunk &input,
	                                          DataChunk &output);
};

} // namespace duckdb
