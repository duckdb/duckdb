#include "duckdb/planner/expression_binder/try_operator_binder.hpp"

#include "duckdb/planner/binder.hpp"

namespace duckdb {

TryOperatorBinder::TryOperatorBinder(Binder &binder, ClientContext &context) : ExpressionBinder(binder, context, true) {
}

BindResult TryOperatorBinder::BindAggregate(FunctionExpression &expr, AggregateFunctionCatalogEntry &function,
                                            idx_t depth) {
	throw BinderException("aggregates are not allowed inside the TRY expression");
}

BindResult TryOperatorBinder::BindExpression(unique_ptr<ParsedExpression> &expr_ptr, idx_t depth,
                                             bool root_expression) {
	if (!stored_binder || expr_ptr->GetExpressionClass() != ExpressionClass::COLUMN_REF) {
		return ExpressionBinder::BindExpression(expr_ptr, depth, root_expression);
	}
	// Route COLUMN_REF through the parent binder so grouping and correlation context is visible.
	// On exception, ~TryOperatorBinder() restores stored_binder as active, so no catch is required here.
	D_ASSERT(&binder.GetActiveBinder() == this);
	binder.SetActiveBinder(*stored_binder);
	auto result = stored_binder->BindExpression(expr_ptr, depth, root_expression);
	binder.SetActiveBinder(*this);
	return result;
}
} // namespace duckdb
