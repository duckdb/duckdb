//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_macro_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/macro_function.hpp"

namespace duckdb {

struct CreateMacroFunctionInfo : public CreateFunctionInfo {
	CreateMacroFunctionInfo() : CreateFunctionInfo(CatalogType::MACRO_FUNCTION_ENTRY) {
	}

	unique_ptr<MacroFunction> function;
};

} // namespace duckdb
