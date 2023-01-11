#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_positional_join.hpp"
#include "duckdb/planner/tableref/bound_pos_join_ref.hpp"
#include "duckdb/planner/expression_binder/lateral_binder.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundPositionalJoinRef &expr) {
	auto left = CreatePlan(*expr.left);
	auto right = CreatePlan(*expr.right);
	if (expr.lateral) {
		// lateral positional join
		return PlanLateralJoin(std::move(left), std::move(right), expr.correlated_columns);
	}
	if (!expr.correlated_columns.empty()) {
		// non-lateral join with correlated columns
		// this happens if there is a positional join in a correlated subquery
		// due to the lateral binder the expression depth of all correlated columns in the "ref.correlated_columns" set
		// is 1 too high
		// we reduce expression depth of all columns in the "ref.correlated_columns" set by 1
		LateralBinder::ReduceExpressionDepth(*right, expr.correlated_columns);
	}
	return LogicalPositionalJoin::Create(std::move(left), std::move(right));
}

} // namespace duckdb
