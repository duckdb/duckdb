//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/optimizer/unnest_rewriter.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/logical_operator.hpp"
#include "duckdb/common/pair.hpp"

namespace duckdb {

class Optimizer;

//! The UnnestRewriterPlanUpdater updates column bindings after changing the operator plan
class UnnestRewriterPlanUpdater : LogicalOperatorVisitor {
public:
	UnnestRewriterPlanUpdater(const idx_t &old_table_idx, const idx_t &new_table_idx)
	    : old_table_idx(old_table_idx), new_table_idx(new_table_idx) {
	}
	//! Update each operator of the plan after moving an UNNEST into a projection
	void VisitOperator(LogicalOperator &op) override;
	//! Visit an expression and update its column bindings after moving and UNNEST into a projection
	void VisitExpression(unique_ptr<Expression> *expression) override;

public:
	idx_t old_table_idx;
	idx_t new_table_idx;
};

//! The UnnestRewriter optimizer traverses the logical operator tree and rewrites duplicate
//! eliminated joins that contain UNNESTs by moving the UNNESTs into the projection of
//! the SELECT
class UnnestRewriter {
public:
	UnnestRewriter() {
	}
	//! Rewrite duplicate eliminated joins with UNNESTs
	unique_ptr<LogicalOperator> Optimize(unique_ptr<LogicalOperator> op);

private:
	//! Find delim joins that contain an UNNEST
	void FindCandidates(unique_ptr<LogicalOperator> *op_ptr, vector<unique_ptr<LogicalOperator> *> &candidates);
	//! Rewrite all delim joins that contain an UNNEST
	pair<idx_t, idx_t> RewriteCandidate(unique_ptr<LogicalOperator> *candidate);
};

} // namespace duckdb
