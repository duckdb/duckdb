//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

//! Function used for determining the return type of a table producing function
typedef unique_ptr<FunctionData> (*table_function_bind_t)(ClientContext &context, vector<Value> &inputs,
                                                          unordered_map<string, Value> &named_parameters,
                                                          vector<LogicalType> &return_types, vector<string> &names);
//! Type used for table-returning function
typedef void (*table_function_t)(ClientContext &context, vector<Value> &input, DataChunk &output,
                                 FunctionData *dataptr);
//! Type used for final (cleanup) function
typedef void (*table_function_final_t)(ClientContext &context, FunctionData *dataptr);

class TableFunction : public SimpleFunction {
public:
	TableFunction(string name, vector<LogicalType> arguments, table_function_bind_t bind, table_function_t function,
	              table_function_final_t final = nullptr, bool supports_projection = false)
	    : SimpleFunction(name, move(arguments)), bind(bind), function(function), final(final),
	      supports_projection(supports_projection) {
	}
	TableFunction(vector<LogicalType> arguments, table_function_bind_t bind, table_function_t function,
	              table_function_final_t final = nullptr, bool supports_projection = false)
	    : TableFunction(string(), move(arguments), bind, function, final, supports_projection) {
	}

	//! The bind function
	table_function_bind_t bind;
	//! The function pointer
	table_function_t function;
	//! Final function pointer
	table_function_final_t final;
	//! Supported named parameters by the function
	unordered_map<string, LogicalType> named_parameters;
	//! Whether or not the table function supports projection
	bool supports_projection;

	string ToString();
};

} // namespace duckdb
