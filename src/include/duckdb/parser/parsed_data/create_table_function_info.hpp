//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_table_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CreateTableFunctionInfo : public CreateFunctionInfo {
	CreateTableFunctionInfo(TableFunctionSet set)
	    : CreateFunctionInfo(CatalogType::TABLE_FUNCTION), functions(move(set.functions)) {
		this->name = set.name;
	}
	CreateTableFunctionInfo(TableFunction function)
	    : CreateFunctionInfo(CatalogType::TABLE_FUNCTION) {
		this->name = function.name;
		functions.push_back(move(function));
	}

	//! The table functions
	vector<TableFunction> functions;
};

} // namespace duckdb
