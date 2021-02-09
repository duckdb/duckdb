//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parsed_data/create_scalar_function_info.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/parsed_data/create_function_info.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CreateScalarFunctionInfo : public CreateFunctionInfo {
	explicit CreateScalarFunctionInfo(ScalarFunction function)
	    : CreateFunctionInfo(CatalogType::SCALAR_FUNCTION_ENTRY) {
		this->name = function.name;
		functions.push_back(function);
	}
	explicit CreateScalarFunctionInfo(ScalarFunctionSet set)
	    : CreateFunctionInfo(CatalogType::SCALAR_FUNCTION_ENTRY), functions(move(set.functions)) {
		this->name = set.name;
		for (auto &func : functions) {
			func.name = set.name;
		}
	}

	vector<ScalarFunction> functions;

public:
	unique_ptr<CreateInfo> Copy() const override {
		ScalarFunctionSet set(name);
		set.functions = functions;
		auto result = make_unique<CreateScalarFunctionInfo>(move(set));
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
