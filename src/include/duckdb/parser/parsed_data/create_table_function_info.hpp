//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_table_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_info.hpp"
#include "duckdb/function/table_function.hpp"

namespace duckdb {

struct CreateTableFunctionInfo : public CreateInfo {
	CreateTableFunctionInfo(TableFunction function) : CreateInfo(CatalogType::TABLE_FUNCTION), function(function) {
		this->name = function.name;
	}

	//! Function name
	string name;
	//! The table function
	TableFunction function;
};

} // namespace duckdb
