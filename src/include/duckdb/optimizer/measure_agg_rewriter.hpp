//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/measure_agg_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/optimizer/optimizer.hpp"

namespace duckdb {

class MeasureAggRewriter {
public:
	explicit MeasureAggRewriter(Optimizer &optimizer) : optimizer(optimizer) {
	}

	//! Rewrites MEASURE expressions in the logical plan into traditional SQL
	void RewriteMeasures(unique_ptr<LogicalOperator> &plan);

private:
	Optimizer &optimizer;
};
} // namespace duckdb
