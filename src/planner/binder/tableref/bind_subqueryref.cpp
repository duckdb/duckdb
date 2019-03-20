#include "planner/binder.hpp"
#include "parser/tableref/subqueryref.hpp"
#include "planner/tableref/bound_subqueryref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(SubqueryRef &ref) {
	auto result = make_unique<BoundSubqueryRef>();
	result->bind_index = GenerateTableIndex();
	result->binder = make_unique<Binder>(context, this);
	result->subquery = result->binder->Bind(*ref.subquery);

	bind_context.AddSubquery(result->bind_index, expr.alias, ref);
	MoveCorrelatedExpressions(*result->binder);
	return move(result);
}
