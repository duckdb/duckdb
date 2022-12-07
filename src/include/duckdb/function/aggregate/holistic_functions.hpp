//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/holistic_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/function/built_in_functions.hpp"

namespace duckdb {

struct QuantileFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ModeFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ApproximateQuantileFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

struct ReservoirQuantileFun {
	static void RegisterFunction(BuiltinFunctions &set);
};

} // namespace duckdb
