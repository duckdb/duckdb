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
	auto &else_expr = BoundExpression::GetExpression(*expr.else_expr);
	auto return_type = else_expr->return_type;
	for (auto &check : expr.case_checks) {
		auto &then_expr = BoundExpression::GetExpression(*check.then_expr);
		return_type = LogicalType::MaxLogicalType(return_type, then_expr->return_type);
	}

	// bind all the individual components of the CASE statement
	auto result = make_uniq<BoundCaseExpression>(return_type);
	for (idx_t i = 0; i < expr.case_checks.size(); i++) {
		auto &check = expr.case_checks[i];
		auto &when_expr = BoundExpression::GetExpression(*check.when_expr);
		auto &then_expr = BoundExpression::GetExpression(*check.then_expr);
		BoundCaseCheck result_check;
		result_check.when_expr =
		    BoundCastExpression::AddCastToType(context, std::move(when_expr), LogicalType::BOOLEAN);
		result_check.then_expr = BoundCastExpression::AddCastToType(context, std::move(then_expr), return_type);
		result->case_checks.push_back(std::move(result_check));
	}
	result->else_expr = BoundCastExpression::AddCastToType(context, std::move(else_expr), return_type);
	return BindResult(std::move(result));
}
} // namespace duckdb
