//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/regex_range_filter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/rule.hpp"
#include "duckdb/function/function.hpp"

namespace duckdb {

class Optimizer;

class LikeOptimizer {
public:
	LikeOptimizer(Optimizer &optimizer) : optimizer(optimizer) {
	}
	Optimizer &optimizer;
	unique_ptr<LogicalOperator> Rewrite(unique_ptr<LogicalOperator> node);

private:
	//! Transform a LIKE in optimized scalar functions such as prefix, suffix, and contains
	unique_ptr<LogicalOperator> ApplyLikeOptimizations(unique_ptr<LogicalOperator> op);
	ScalarFunction GetScalarFunction(string func_name);
};

} // namespace duckdb
