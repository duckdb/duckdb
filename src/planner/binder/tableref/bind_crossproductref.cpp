#include "duckdb/parser/tableref/crossproductref.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/tableref/bound_crossproductref.hpp"
#include "duckdb/planner/expression_binder/where_binder.hpp"
#include "duckdb/planner/expression_binder/lateral_binder.hpp"

namespace duckdb {

unique_ptr<BoundTableRef> Binder::Bind(CrossProductRef &ref) {
	auto result = make_unique<BoundCrossProductRef>();
	result->left_binder = Binder::CreateBinder(context, this);
	result->right_binder = Binder::CreateBinder(context, result->left_binder.get());
	auto &left_binder = *result->left_binder;
	auto &right_binder = *result->right_binder;

	result->left = left_binder.Bind(*ref.left);
	{
		LateralBinder binder(left_binder, context);
		result->right = right_binder.Bind(*ref.right);
		result->lateral = binder.HasCorrelatedColumns();
		result->correlated_columns = binder.ExtractCorrelatedColumns(right_binder);
	}

	bind_context.AddContext(move(left_binder.bind_context));
	bind_context.AddContext(move(right_binder.bind_context));
	MoveCorrelatedExpressions(left_binder);
	MoveCorrelatedExpressions(right_binder);
	return move(result);
}

} // namespace duckdb
