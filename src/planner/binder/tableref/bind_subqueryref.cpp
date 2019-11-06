#include "duckdb/parser/tableref/subqueryref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_subqueryref.hpp"

using namespace duckdb;
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(SubqueryRef &ref) {
	auto binder = make_unique<Binder>(context, this);
	auto subquery = binder->Bind(*ref.subquery);
	auto bind_index = GenerateTableIndex();
	auto result = make_unique<BoundSubqueryRef>(move(binder), move(subquery), bind_index);

	bind_context.AddSubquery(result->bind_index, ref.alias, ref, *result->subquery);
	MoveCorrelatedExpressions(*result->binder);
	return move(result);
}
