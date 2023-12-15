//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/topn_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/constants.hpp"

namespace duckdb {
class LogicalOperator;
class Optimizer;

class TopN {
public:
	//! Optimize ORDER BY + LIMIT to TopN
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);
	//! Whether we can perform the optimization on this operator
	static bool CanOptimize(LogicalOperator &op);
};

} // namespace duckdb
