#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/tableref/bound_cteref.hpp"

namespace duckdb {

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCTERef &ref) {
	auto index = ref.bind_index;

	vector<LogicalType> types;
	types.reserve(ref.types.size());
	for (auto &type : ref.types) {
		types.push_back(type);
	}

	return make_uniq<LogicalCTERef>(index, ref.cte_index, types, ref.bound_columns, ref.materialized_cte);
}

} // namespace duckdb
