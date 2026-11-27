#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(CaseExpression &expr, idx_t depth) {
	ErrorData error;
	unique_ptr<Expression> operand;
	bool share_operand = false;
	vector<pair<unique_ptr<Expression>, unique_ptr<Expression>>> checks;
	if (expr.CaseOperand()) {
		operand = BindChild(expr.CaseOperandMutable(), depth, error);
		if (error.HasError()) {
			return BindResult(std::move(error));
		}
		// Even without a volatile scalar call, a subquery can sample or choose an unordered row.
		share_operand = operand->IsVolatile() || operand->HasSubquery();
	}
	for (auto &arm : expr.SimpleChecksMutable()) {
		vector<unique_ptr<Expression>> conditions;
		for (auto &test : arm.when) {
			auto rhs = BindChild(test.expression, depth, error);
			if (!rhs) {
				return BindResult(std::move(error));
			}
			unique_ptr<Expression> lhs;
			if (share_operand) {
				lhs = make_uniq<BoundCaseOperandExpression>(operand->GetReturnType());
			} else {
				lhs = operand->Copy();
			}
			auto comparison = CreateBoundComparison(test.comparison_type, std::move(lhs), std::move(rhs), error);
			if (!comparison) {
				error.AddQueryLocation(expr);
				return BindResult(std::move(error));
			}
			conditions.push_back(std::move(comparison));
		}
		auto condition = BoundConjunctionExpression::Create(ExpressionType::CONJUNCTION_OR, std::move(conditions));
		checks.emplace_back(std::move(condition), BindChild(arm.then_expr, depth, error));
	}
	for (auto &check : expr.CaseChecksMutable()) {
		auto when_expr = BindChild(check.when_expr, depth, error);
		auto then_expr = BindChild(check.then_expr, depth, error);
		checks.emplace_back(std::move(when_expr), std::move(then_expr));
	}
	auto else_expr = BindChild(expr.ElseMutable(), depth, error);
	if (error.HasError()) {
		return BindResult(std::move(error));
	}
	// the children have been successfully resolved
	// figure out the result type of the CASE expression
	auto return_type = ExpressionBinder::GetExpressionReturnType(*else_expr);
	const auto decimal_width_limit = binder.GetDecimalWidthLimit();
	for (auto &check : checks) {
		auto then_type = ExpressionBinder::GetExpressionReturnType(*check.second);
		if (!LogicalType::TryGetMaxLogicalType(context, return_type, then_type, return_type, decimal_width_limit)) {
			throw BinderException(
			    expr, "Cannot mix values of type %s and %s in CASE expression - an explicit cast is required",
			    return_type.ToString(), then_type.ToString());
		}
	}

	// bind all the individual components of the CASE statement
	auto result = make_uniq<BoundCaseExpression>(return_type);
	for (auto &check : checks) {
		BoundCaseCheck result_check;
		result_check.when_expr =
		    BoundCastExpression::AddCastToType(context, std::move(check.first), LogicalType::BOOLEAN);
		result_check.then_expr = BoundCastExpression::AddCastToType(context, std::move(check.second), return_type);
		result->CaseChecksMutable().push_back(std::move(result_check));
	}
	result->ElseMutable() = BoundCastExpression::AddCastToType(context, std::move(else_expr), return_type);
	if (share_operand) {
		result->OperandMutable() = std::move(operand);
	}
	return BindResult(std::move(result));
}
} // namespace duckdb
