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
	    : CreateFunctionInfo(CatalogType::SCALAR_FUNCTION_ENTRY), functions(function.name) {
		name = function.name;
		functions.AddFunction(move(function));
	}
	explicit CreateScalarFunctionInfo(ScalarFunctionSet set)
	    : CreateFunctionInfo(CatalogType::SCALAR_FUNCTION_ENTRY), functions(move(set)) {
		name = functions.name;
		for (auto &func : functions.functions) {
			func.name = functions.name;
		}
	}

	ScalarFunctionSet functions;

public:
	unique_ptr<CreateInfo> Copy() const override {
		ScalarFunctionSet set(name);
		set.functions = functions.functions;
		auto result = make_unique<CreateScalarFunctionInfo>(move(set));
		CopyProperties(*result);
		return move(result);
	}
};

} // namespace duckdb
