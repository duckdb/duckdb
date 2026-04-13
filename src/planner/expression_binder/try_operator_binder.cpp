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
	if (stored_binder && expr_ptr->GetExpressionClass() == ExpressionClass::COLUMN_REF) {
		auto &active_binder = binder.GetActiveBinder();
		D_ASSERT(&active_binder == this);
		binder.SetActiveBinder(*stored_binder);
		try {
			auto result = stored_binder->BindExpression(expr_ptr, depth, root_expression);
			binder.SetActiveBinder(active_binder);
			return result;
		} catch (...) {
			binder.SetActiveBinder(active_binder);
			throw;
		}
	}
	return ExpressionBinder::BindExpression(expr_ptr, depth, root_expression);
}
} // namespace duckdb
