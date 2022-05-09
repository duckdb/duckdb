//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/nested_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"
#include "duckdb/function/scalar_function.hpp"
#include "duckdb/common/map.hpp"
#include "duckdb/common/unordered_map.hpp"

namespace duckdb {

struct VariableReturnBindData : public FunctionData {
	LogicalType stype;

	explicit VariableReturnBindData(const LogicalType &stype_p) : stype(stype_p) {
	}

	unique_ptr<FunctionData> Copy() const override {
		return make_unique<VariableReturnBindData>(stype);
	}
	bool Equals(const FunctionData &other_p) const override {
		auto &other = (const VariableReturnBindData &)other_p;
		return stype == other.stype;
	}
};

template <class T, class MAP_TYPE = map<T, idx_t>>
struct HistogramAggState {
	MAP_TYPE *hist;
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

struct ListRangeFun {
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

struct ListConcatFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListContainsFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListFlattenFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListPositionFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListAggregateFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListDistinctFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListUniqueFun {
	static ScalarFunction GetFunction();
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ListSortFun {
	static ScalarFunction GetFunction();
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
