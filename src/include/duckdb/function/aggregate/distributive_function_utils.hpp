//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/function/aggregate/distributive_functions.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/function/function_set.hpp"

namespace duckdb {

struct CountFunctionBase {
	static AggregateFunction GetFunction();
};

struct FirstFunctionGetter {
	static AggregateFunction GetFunction(const LogicalType &type);
};

struct MinFunction {
	static AggregateFunction GetFunction();
};

struct MaxFunction {
	static AggregateFunction GetFunction();
};

} // namespace duckdb
