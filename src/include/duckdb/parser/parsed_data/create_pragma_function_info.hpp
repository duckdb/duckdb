//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_pragma_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/pragma_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CreatePragmaFunctionInfo : public CreateFunctionInfo {
	explicit CreatePragmaFunctionInfo(PragmaFunction function)
	    : CreateFunctionInfo(CatalogType::PRAGMA_FUNCTION_ENTRY), functions(function.name) {
		name = function.name;
		functions.AddFunction(move(function));
	}
	CreatePragmaFunctionInfo(string name, PragmaFunctionSet functions_)
	    : CreateFunctionInfo(CatalogType::PRAGMA_FUNCTION_ENTRY), functions(functions_) {
		this->name = name;
	}

	PragmaFunctionSet functions;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreatePragmaFunctionInfo>(functions.name, functions);
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
