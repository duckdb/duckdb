#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(ConjunctionExpression &expr, idx_t depth) {
	// first try to bind the children of the case expression
	ErrorData error;
	for (idx_t i = 0; i < expr.GetChildrenMutable().size(); i++) {
		BindChild(expr.GetChildrenMutable()[i], depth, error);
	}
	if (error.HasError()) {
		return BindResult(std::move(error));
	}
	// the children have been successfully resolved
	// cast the input types to boolean (if necessary)
	// and construct the bound conjunction expression
	auto result = make_uniq<BoundConjunctionExpression>(expr.GetExpressionType());
	for (auto &child_expr : expr.GetChildrenMutable()) {
		auto &child = BoundExpression::GetExpression(*child_expr);
		result->GetChildrenMutable().push_back(
		    BoundCastExpression::AddCastToType(context, std::move(child), LogicalType::BOOLEAN));
	}
	// now create the bound conjunction expression
	return BindResult(std::move(result));
}

} // namespace duckdb
