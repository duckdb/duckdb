#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(SubqueryRef &ref, optional_ptr<CommonTableExpressionInfo> cte) {
	auto binder = Binder::CreateBinder(context, this);
	binder->can_contain_nulls = true;
	if (cte) {
		binder->bound_ctes.insert(*cte);
	}
	auto subquery = binder->BindNode(*ref.subquery->node);
	binder->alias = ref.alias.empty() ? "unnamed_subquery" : ref.alias;
	idx_t bind_index = subquery->GetRootIndex();
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
	auto result = make_uniq<BoundSubqueryRef>(std::move(binder), std::move(subquery));
	bind_context.AddSubquery(bind_index, subquery_alias, ref, *result->subquery);
	MoveCorrelatedExpressions(*result->binder);
	return std::move(result);
}

} // namespace duckdb
