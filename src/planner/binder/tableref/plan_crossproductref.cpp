#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_cross_product.hpp"
#include "duckdb/planner/tableref/bound_crossproductref.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCrossProductRef &expr) {
	auto left = CreatePlan(*expr.left);
	auto right = CreatePlan(*expr.right);

	return LogicalCrossProduct::Create(move(left), move(right));
}

} // namespace duckdb
