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

namespace duckdb {

struct CreatePragmaFunctionInfo : public CreateFunctionInfo {
	explicit CreatePragmaFunctionInfo(PragmaFunction function)
	    : CreateFunctionInfo(CatalogType::PRAGMA_FUNCTION_ENTRY) {
		functions.push_back(move(function));
		this->name = function.name;
	}
	CreatePragmaFunctionInfo(string name, vector<PragmaFunction> functions_)
	    : CreateFunctionInfo(CatalogType::PRAGMA_FUNCTION_ENTRY), functions(move(functions_)) {
		this->name = name;
		for (auto &function : functions) {
			function.name = name;
		}
	}

	vector<PragmaFunction> functions;

public:
	unique_ptr<CreateInfo> Copy() const override {
		auto result = make_unique<CreatePragmaFunctionInfo>(functions[0].name, functions);
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
