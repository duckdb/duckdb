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

struct BuildSize {
	idx_t left_side;
	idx_t right_side;

	// Initialize with 1 so the build side is just the cardinality if types aren't
	// known.
	BuildSize() : left_side(1), right_side(1) {
	}
};

class BuildProbeSideOptimizer : LogicalOperatorVisitor {
public:
	explicit BuildProbeSideOptimizer(ClientContext &context);

	void VisitOperator(LogicalOperator &op) override;
	void VisitExpression(unique_ptr<Expression> *expression) override {};

	void TryFlipJoinChildren(LogicalOperator &op, idx_t cardinality_ratio = 1);

	BuildSize GetBuildSide(LogicalOperator &op);

private:
	ClientContext &context;
};

} // namespace duckdb
