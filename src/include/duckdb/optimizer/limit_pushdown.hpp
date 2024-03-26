//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/limit_pushdown.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class LogicalOperator;
class Optimizer;

class LimitPushdown {
public:
	//! Optimize PROJECTION + LIMIT to LIMIT + Projection
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Whether we can perform the optimization on this operator
	static bool CanOptimize(LogicalOperator &op);
};

} // namespace duckdb
