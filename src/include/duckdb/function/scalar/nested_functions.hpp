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

	explicit VariableReturnBindData(LogicalType stype) : stype(stype) {
	}

	unique_ptr<FunctionData> Copy() override {
		return make_unique<VariableReturnBindData>(stype);
	}
};

struct ArraySliceFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StructPackFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListValueFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MapFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct MapExtractFun {
	static void RegisterFunction(BuiltinFunctions &set);
};
struct ListExtractFun {
	static void RegisterFunction(BuiltinFunctions &set);
};
struct CardinalityFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct StructExtractFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
