//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/pragma_function.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function.hpp"
#include "duckdb/parser/parsed_data/pragma_info.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {
class ClientContext;

//! Return a substitute query to execute instead of this pragma statement
typedef string (*pragma_query_t)(ClientContext &context, const FunctionParameters &parameters);
//! Execute the main pragma function
typedef void (*pragma_function_t)(ClientContext &context, const FunctionParameters &parameters);

//! Pragma functions are invoked by calling PRAGMA x
//! Pragma functions come in three types:
//! * Call: function call, e.g. PRAGMA table_info('tbl')
//!   -> call statements can take multiple parameters
//! * Statement: statement without parameters, e.g. PRAGMA show_tables
//!   -> this is similar to a call pragma but without parameters
//! Pragma functions can either return a new query to execute (pragma_query_t)
//! or they can
class PragmaFunction : public SimpleNamedParameterFunction {
public:
	// Call
	DUCKDB_API static PragmaFunction PragmaCall(const string &name, pragma_query_t query, vector<LogicalType> arguments,
	                                            LogicalType varargs = LogicalType::INVALID);
	DUCKDB_API static PragmaFunction PragmaCall(const string &name, pragma_function_t function,
	                                            vector<LogicalType> arguments,
	                                            LogicalType varargs = LogicalType::INVALID);
	// Statement
	DUCKDB_API static PragmaFunction PragmaStatement(const string &name, pragma_query_t query);
	DUCKDB_API static PragmaFunction PragmaStatement(const string &name, pragma_function_t function);

	DUCKDB_API string ToString() const override;

public:
	PragmaType type;

	pragma_query_t query;
	pragma_function_t function;
	named_parameter_type_map_t named_parameters;

private:
	PragmaFunction(string name, PragmaType pragma_type, pragma_query_t query, pragma_function_t function,
	               vector<LogicalType> arguments, LogicalType varargs);
};

} // namespace duckdb
