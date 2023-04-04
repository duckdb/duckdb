#include "duckdb/planner/tableref/bound_pivotref.hpp"
#include "duckdb/planner/operator/logical_pivot.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundPivotRef &ref) {
	auto subquery = ref.child_binder->CreatePlan(*ref.child);

	auto result = make_uniq<LogicalPivot>(ref.bind_index, std::move(subquery), std::move(ref.bound_pivot));
	return std::move(result);
}

} // namespace duckdb
