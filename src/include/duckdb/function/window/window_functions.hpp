//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/window/window_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct WindowFunctions {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RowNumberFunc {
	static WindowFunction GetFunction();
};

struct NtileFunc {
	static WindowFunction GetFunction();
};

struct RankFunc {
	static WindowFunction GetFunction();
};

struct DenseRankFun {
	static WindowFunction GetFunction();
};

struct RankDenseFun {
	static WindowFunction GetFunction();
};

struct PercentRankFun {
	static WindowFunction GetFunction();
};

struct CumeDistFun {
	static WindowFunction GetFunction();
};

struct FirstValueFun {
	static WindowFunction GetFunction();
};

struct LastValueFun {
	static WindowFunction GetFunction();
};

struct NthValueFun {
	static WindowFunction GetFunction();
};

struct LeadFun {
	static WindowFunctionSet GetFunctions();

	static WindowFunction GetTypedFunction(const LogicalType &type, idx_t nargs);
};

struct LagFun {
	static WindowFunctionSet GetFunctions();
};

struct FillFun {
	static WindowFunction GetFunction();
};

} // namespace duckdb
