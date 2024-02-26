#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/tableref/bound_cteref.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCTERef &ref) {
	return make_uniq<LogicalCTERef>(ref.bind_index, ref.cte_index, ref.types, ref.bound_columns, ref.materialized_cte);
}

} // namespace duckdb
