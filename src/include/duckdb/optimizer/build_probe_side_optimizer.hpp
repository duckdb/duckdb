//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/build_side_probe_side_optimizer.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/common/unordered_set.hpp"
#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/planner/operator/logical_filter.hpp"
#include "duckdb/common/vector.hpp"

namespace duckdb {

class BuildProbeSideOptimizer : LogicalOperatorVisitor {
public:
	explicit BuildProbeSideOptimizer(ClientContext &context);

	void VisitOperator(LogicalOperator &op) override;
	void VisitExpression(unique_ptr<Expression> *expression) override {};

	void TryFlipChildren(LogicalOperator &op, idx_t cardinality_ratio = 1);
private:
	ClientContext &context;
};

} // namespace duckdb
