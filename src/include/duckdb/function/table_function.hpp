//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"

namespace duckdb {

//! Type used for initialization function
typedef FunctionData *(*table_function_init_t)(ClientContext &);
//! Type used for table-returning function
typedef void (*table_function_t)(ClientContext &, DataChunk &input, DataChunk &output, FunctionData *dataptr);
//! Type used for final (cleanup) function
typedef void (*table_function_final_t)(ClientContext &, FunctionData *dataptr);

class TableFunction : public Function {
public:
	TableFunction(string name, vector<SQLType> arguments, vector<SQLType> return_types, vector<string> names,
	              table_function_init_t init, table_function_t function, table_function_final_t final)
	    : Function(name), types(move(return_types)), names(move(names)), arguments(move(arguments)), init(init),
	      function(function), final(final) {
	}

	//! List of return column types
	vector<SQLType> types;
	//! List of return column names
	vector<string> names;
	//! Input arguments
	vector<SQLType> arguments;
	//! Initialize function pointer
	table_function_init_t init;
	//! The function pointer
	table_function_t function;
	//! Final function pointer
	table_function_final_t final;
};

} // namespace duckdb
