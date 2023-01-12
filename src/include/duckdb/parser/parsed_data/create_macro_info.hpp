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
	CreateMacroInfo() : CreateFunctionInfo(CatalogType::MACRO_ENTRY, INVALID_SCHEMA) {
	}

	CreateMacroInfo(CatalogType type) : CreateFunctionInfo(type, INVALID_SCHEMA) {
	}

	unique_ptr<MacroFunction> function;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreateMacroInfo>();
		result->function = function->Copy();
		result->name = name;
		CopyProperties(*result);
		return std::move(result);
	}
};

} // namespace duckdb
