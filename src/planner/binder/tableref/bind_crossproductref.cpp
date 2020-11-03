#include "duckdb/parser/tableref/crossproductref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_crossproductref.hpp"

namespace duckdb {
using namespace std;

unique_ptr<BoundTableRef> Binder::Bind(CrossProductRef &ref) {
	auto result = make_unique<BoundCrossProductRef>();
	Binder left_binder(context, this);
	Binder right_binder(context, this);

	result->left = left_binder.Bind(*ref.left);
	result->right = right_binder.Bind(*ref.right);

	bind_context.AddContext(move(left_binder.bind_context));
	bind_context.AddContext(move(right_binder.bind_context));
	MoveCorrelatedExpressions(left_binder);
	MoveCorrelatedExpressions(right_binder);
	return move(result);
}

} // namespace duckdb
