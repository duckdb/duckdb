//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_macro_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"

namespace duckdb {

struct CreateMacroFunctionInfo : public CreateFunctionInfo {
	CreateMacroFunctionInfo() : CreateFunctionInfo(CatalogType::MACRO_FUNCTION_ENTRY) {
	}

	vector<unique_ptr<ParsedExpression>> arguments;
	unique_ptr<ParsedExpression> function;
};

} // namespace duckdb
