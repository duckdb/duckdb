//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_macro_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/macro_function.hpp"

namespace duckdb {

struct CreateMacroInfo : public CreateFunctionInfo {
	CreateMacroInfo() : CreateFunctionInfo(CatalogType::MACRO_ENTRY) {
	}

	unique_ptr<MacroFunction> function;

public:
	unique_ptr<CreateInfo> Copy() const override {
		throw NotImplementedException("FIXME: copy CreateMacroInfo");
	}
};

} // namespace duckdb
