//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_table_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/parse_info.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct CreateTableFunctionInfo : public ParseInfo {
	CreateTableFunctionInfo(TableFunction function) : schema(DEFAULT_SCHEMA), or_replace(false), function(function) {
		this->name = function.name;
	}

	//! Schema name
	string schema;
	//! Function name
	string name;
	//! Replace function if it already exists instead of failing
	bool or_replace = false;
	//! The table function
	TableFunction function;
};

} // namespace duckdb
