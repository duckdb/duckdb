#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(CaseExpression &expr, idx_t depth) {
	// first try to bind the children of the case expression
	string error;
	for (auto &check : expr.case_checks) {
		BindChild(check.when_expr, depth, error);
		BindChild(check.then_expr, depth, error);
	}
	BindChild(expr.else_expr, depth, error);
	if (!error.empty()) {
		return BindResult(error);
	}
	// the children have been successfully resolved
	// figure out the result type of the CASE expression
	auto return_type = ((BoundExpression &)*expr.else_expr).expr->return_type;
	for (auto &check : expr.case_checks) {
		auto &then_expr = (BoundExpression &)*check.then_expr;
		return_type = LogicalType::MaxLogicalType(return_type, then_expr.expr->return_type);
	}
	ExpressionBinder::ResolveParameterType(return_type);

	// now rewrite the case into a chain of cases

	// CASE WHEN e1 THEN r1 WHEN w2 THEN r2 ELSE r3 is rewritten to
	// CASE WHEN e1 THEN r1 ELSE CASE WHEN e2 THEN r2 ELSE r3

	auto root = make_unique<BoundCaseExpression>(return_type);
	auto current_root = root.get();
	for (idx_t i = 0; i < expr.case_checks.size(); i++) {
		auto &check = expr.case_checks[i];
		auto &when_expr = (BoundExpression &)*check.when_expr;
		auto &then_expr = (BoundExpression &)*check.then_expr;
		current_root->check = BoundCastExpression::AddCastToType(move(when_expr.expr), LogicalType::BOOLEAN);
		current_root->result_if_true = BoundCastExpression::AddCastToType(move(then_expr.expr), return_type);
		if (i + 1 == expr.case_checks.size()) {
			// finished all cases
			// res_false is the default result
			auto &else_expr = (BoundExpression &)*expr.else_expr;
			current_root->result_if_false = BoundCastExpression::AddCastToType(move(else_expr.expr), return_type);
		} else {
			// more cases remain, create a case statement within the FALSE branch
			auto next_case = make_unique<BoundCaseExpression>(return_type);
			auto case_ptr = next_case.get();
			current_root->result_if_false = move(next_case);
			current_root = case_ptr;
		}
	}
	return BindResult(move(root));
}
} // namespace duckdb
