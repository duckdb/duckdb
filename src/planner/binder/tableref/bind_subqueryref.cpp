#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BoundStatement Binder::Bind(SubqueryRef &ref) {
	auto binder = Binder::CreateBinder(context, this);
	binder->can_contain_nulls = true;
	auto subquery = binder->BindNode(*ref.subquery->node);
	binder->alias = ref.alias.empty() ? "unnamed_subquery" : ref.alias;
	idx_t bind_index = subquery.plan->GetRootIndex();
	string subquery_alias;
	if (ref.alias.empty()) {
		auto index = unnamed_subquery_index++;
		subquery_alias = "unnamed_subquery";
		;
		if (index > 1) {
			subquery_alias += to_string(index);
		}
	} else {
		subquery_alias = ref.alias;
	}
	binder->is_outside_flattened = is_outside_flattened;
	if (binder->has_unplanned_dependent_joins) {
		has_unplanned_dependent_joins = true;
	}
	bind_context.AddSubquery(bind_index, subquery_alias, ref, subquery);
	MoveCorrelatedExpressions(*binder);

	return subquery;
}

} // namespace duckdb
