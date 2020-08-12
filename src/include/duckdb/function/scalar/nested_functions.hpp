//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/nested_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct VariableReturnBindData : public FunctionData {
	LogicalType stype;

	VariableReturnBindData(LogicalType stype) : stype(stype) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<VariableReturnBindData>(stype);
	}
};

struct StructPackFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListValueFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StructExtractBindData : public FunctionData {
	string key;
	idx_t index;
	PhysicalType type;

	StructExtractBindData(string key, idx_t index, PhysicalType type) : key(key), index(index), type(type) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<StructExtractBindData>(key, index, type);
	}
};

struct StructExtractFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
