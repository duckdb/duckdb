#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(SubqueryRef &ref, CommonTableExpressionInfo *cte) {
	auto binder = Binder::CreateBinder(context, this);
	binder->can_contain_nulls = true;
	if (cte) {
		binder->bound_ctes.insert(cte);
	}
	binder->alias = ref.alias.empty() ? "unnamed_subquery" : ref.alias;
	auto subquery = binder->BindNode(*ref.subquery->node);
	idx_t bind_index = subquery->GetRootIndex();
	string alias;
	if (ref.alias.empty()) {
		alias = "unnamed_subquery" + to_string(bind_index);
	} else {
		alias = ref.alias;
	}
	auto result = make_unique<BoundSubqueryRef>(std::move(binder), std::move(subquery));
	bind_context.AddSubquery(bind_index, alias, ref, *result->subquery);
	MoveCorrelatedExpressions(*result->binder);
	return std::move(result);
}

} // namespace duckdb
