//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/scalar/statistical_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/scalar_function.hpp"
#include "duckdb/function/function_set.hpp"

namespace duckdb {
	struct StatisticalNormalPDFFun {
		static void RegisterFunction(BuiltinFunctions &set);
	};
} // namespace duckdb