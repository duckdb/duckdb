#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/tableref/bound_crossproductref.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCrossProductRef &expr) {
	auto left = CreatePlan(*expr.left);
	auto right = CreatePlan(*expr.right);
	if (expr.lateral) {
		// lateral cross product
		return PlanLateralJoin(move(left), move(right), expr.correlated_columns);
	}
	if (!expr.correlated_columns.empty()) {
		// non-lateral join with correlated columns
		// this happens if there is a cross product in a correlated subquery
		// due to the lateral binder the expression depth of all correlated columns in the "ref.correlated_columns" set
		// is 1 too high
		// we reduce expression depth of all columns in the "ref.correlated_columns" set by 1
		LateralBinder::ReduceExpressionDepth(*right, expr.correlated_columns);
	}
	return LogicalCrossProduct::Create(move(left), move(right));
}

} // namespace duckdb
