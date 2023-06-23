#include "duckdb/parser/expression/conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(ConjunctionExpression &expr, idx_t depth) {
	// first try to bind the children of the case expression
	string error;
	for (idx_t i = 0; i < expr.children.size(); i++) {
		BindChild(expr.children[i], depth, error);
	}
	if (!error.empty()) {
		return BindResult(error);
	}
	// the children have been successfully resolved
	// cast the input types to boolean (if necessary)
	// and construct the bound conjunction expression
	auto result = make_uniq<BoundConjunctionExpression>(expr.type);
	for (auto &child_expr : expr.children) {
		auto &child = BoundExpression::GetExpression(*child_expr);
		result->children.push_back(BoundCastExpression::AddCastToType(context, std::move(child), LogicalType::BOOLEAN));
	}
	// now create the bound conjunction expression
	return BindResult(std::move(result));
}

} // namespace duckdb
