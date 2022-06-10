//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/nested_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/aggregate_function.hpp"
#include "duckdb/function/function_set.hpp"
#include "duckdb/planner/expression/bound_aggregate_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

struct ListFun {
	static void RegisterFunction(BuiltinFunctions &set);
};
struct HistogramFun {
	static void RegisterFunction(BuiltinFunctions &set);
	static AggregateFunction GetHistogramUnorderedMap(LogicalType &type);
};

} // namespace duckdb
