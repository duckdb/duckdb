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
typedef unique_ptr<FunctionData> (*table_function_init_t)(ClientContext &);
//! Type used for table-returning function
typedef void (*table_function_t)(ClientContext &context, vector<Value> &input, DataChunk &output, FunctionData *dataptr);
//! Type used for final (cleanup) function
typedef void (*table_function_final_t)(ClientContext &context, FunctionData *dataptr);
//! Function used for determining the return type of a table producing function
typedef void (*table_function_bind_t)(vector<Value> inputs, vector<SQLType> &return_types, vector<string> &names);

class TableFunction : public Function {
public:
	TableFunction(string name, vector<SQLType> arguments, table_function_bind_t bind, table_function_init_t init,
	              table_function_t function, table_function_final_t final)
	    : Function(name), arguments(move(arguments)), bind(bind), init(init), function(function), final(final) {
	}

	//! Input arguments
	vector<SQLType> arguments;
	//! The bind function
	table_function_bind_t bind;
	//! Initialize function pointer
	table_function_init_t init;
	//! The function pointer
	table_function_t function;
	//! Final function pointer
	table_function_final_t final;
};

} // namespace duckdb
