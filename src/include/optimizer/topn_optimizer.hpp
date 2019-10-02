//===----------------------------------------------------------------------===//
//                         DuckDB
//
// optimizer/topn_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

namespace duckdb {
class Optimizer;

class TopN {
public:
	//! Optimize ORDER BY + LIMIT to TopN
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> node);
};

} // namespace duckdb
