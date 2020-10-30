//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_sql_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"

namespace duckdb {

struct CreateSQLFunctionInfo : public CreateFunctionInfo {
	CreateSQLFunctionInfo() : CreateFunctionInfo(CatalogType::SCALAR_FUNCTION_ENTRY) {
	}

	vector<unique_ptr<ParsedExpression>> arguments;
	unique_ptr<ParsedExpression> function;
};

} // namespace duckdb
