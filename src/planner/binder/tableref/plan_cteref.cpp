#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/operator/logical_cteref.hpp"
#include "duckdb/planner/tableref/bound_cteref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<LogicalOperator> Binder::CreatePlan(BoundCTERef &ref) {
	auto index = ref.bind_index;

	vector<TypeId> types;
	for (auto &expr : ref.types) {
		types.push_back(GetInternalType(expr.id));
	}

	return make_unique<LogicalCTERef>(index, ref.cte_index, types, ref.bound_columns);
}
