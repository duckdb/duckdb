#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/execution/expression_executor.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(FilterExpression &expr, idx_t depth) {
	// first try to bind the children of the case expression
	string error;
	BindChild(expr.filter, depth, error);
	if (!error.empty()) {
		return BindResult(error);
	}
	auto &filter = (BoundExpression &)*expr.filter;
	return BindResult(
	    make_unique<BoundFilterExpression>(move(filter.expr)));
}
} // namespace duckdb
