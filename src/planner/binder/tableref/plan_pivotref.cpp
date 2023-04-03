#include "duckdb/planner/tableref/bound_pivotref.hpp"
#include "duckdb/planner/operator/logical_pivot.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundPivotRef &ref) {
	auto subquery = ref.child_binder->CreatePlan(*ref.child);

	auto result = make_unique<LogicalPivot>(ref.bind_index, std::move(subquery));
	result->return_types = std::move(ref.types);
	result->pivot_values = std::move(ref.pivot_values);
	return result;
}

}