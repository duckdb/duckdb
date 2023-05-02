//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/table/range.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/table_function.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct CheckpointFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct GlobTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RangeTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RepeatTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct RepeatRowTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct UnnestTableFunction {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
