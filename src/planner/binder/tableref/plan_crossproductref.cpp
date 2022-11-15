#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/tableref/bound_crossproductref.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCrossProductRef &expr) {
	auto left = CreatePlan(*expr.left);
	auto right = CreatePlan(*expr.right);
	if (expr.correlated_columns.empty()) {
		return LogicalCrossProduct::Create(move(left), move(right));
	}
	return PlanLateralJoin(move(left), move(right), expr.correlated_columns);
}

} // namespace duckdb
