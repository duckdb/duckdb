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

struct ReplaceBinding {
	ReplaceBinding() {};
	ReplaceBinding(ColumnBinding old_binding, ColumnBinding new_binding)
	    : old_binding(old_binding), new_binding(new_binding) {
	}
	ColumnBinding old_binding;
	ColumnBinding new_binding;
};

struct LHSBinding {
	LHSBinding() {};
	LHSBinding(ColumnBinding binding, LogicalType type) : binding(binding), type(type) {
	}
	ColumnBinding binding;
	LogicalType type;
	string alias;
};

//! The UnnestRewriterPlanUpdater updates column bindings after changing the operator plan
class UnnestRewriterPlanUpdater : LogicalOperatorVisitor {
public:
	UnnestRewriterPlanUpdater() {
	}
	//! Update each operator of the plan after moving an UNNEST into a projection
	void VisitOperator(LogicalOperator &op) override;
	//! Visit an expression and update its column bindings after moving and UNNEST into a projection
	void VisitExpression(unique_ptr<Expression> *expression) override;

	//! Contains all bindings that need to be updated
	vector<ReplaceBinding> replace_bindings;
	//! Stores the table index of the former child of the LOGICAL_UNNEST
	idx_t overwritten_tbl_idx;
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
	//! Rewrite a delim join that contains an UNNEST
	bool RewriteCandidate(unique_ptr<LogicalOperator> *candidate);
	//! Update the bindings of the RHS sequence of LOGICAL_PROJECTION(s)
	void UpdateRHSBindings(unique_ptr<LogicalOperator> *plan_ptr, unique_ptr<LogicalOperator> *candidate,
	                       UnnestRewriterPlanUpdater &updater);
	//! Update the bindings of the BOUND_UNNEST expression of the LOGICAL_UNNEST
	void UpdateBoundUnnestBindings(UnnestRewriterPlanUpdater &updater, unique_ptr<LogicalOperator> *candidate);

	//! Store all delim columns of the delim join
	void GetDelimColumns(LogicalOperator &op);
	//! Store all LHS expressions of the LOGICAL_PROJECTION
	void GetLHSExpressions(LogicalOperator &op);

	//! Keep track of the delim columns to find the correct UNNEST column
	vector<ColumnBinding> delim_columns;
	//! Store the column bindings of the LHS child of the LOGICAL_DELIM_JOIN
	vector<LHSBinding> lhs_bindings;
	//! Stores the table index of the former child of the LOGICAL_UNNEST
	idx_t overwritten_tbl_idx;
	//! The number of distinct columns to unnest
	idx_t distinct_unnest_count;
};

} // namespace duckdb
