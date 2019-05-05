//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parsed_data/create_table_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "function/function.hpp"
#include "parser/column_definition.hpp"

namespace duckdb {

struct CreateTableFunctionInfo {
	//! Schema name
	string schema;
	//! Function name
	string name;
	//! Replace function if it already exists instead of failing
	bool or_replace = false;
	//! List of return columns
	vector<ColumnDefinition> return_values;
	//! Input arguments
	vector<SQLType> arguments;
	//! Initialize function pointer
	table_function_init_t init;
	//! The function pointer
	table_function_t function;
	//! Final function pointer
	table_function_final_t final;

	CreateTableFunctionInfo() : schema(DEFAULT_SCHEMA), or_replace(false) {
	}
};

} // namespace duckdb
