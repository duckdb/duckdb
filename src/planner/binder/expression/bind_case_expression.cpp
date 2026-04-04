#include "duckdb/parser/expression/case_expression.hpp"
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_cast_expression.hpp"
#include "duckdb/planner/expression_binder.hpp"
#include "duckdb/planner/binder.hpp"

namespace duckdb {

BindResult ExpressionBinder::BindExpression(CaseExpression &expr, idx_t depth) {
	// Bind checks in order and prune checks that are provably unreachable.
	ErrorData error;
	vector<BoundCaseCheck> bound_checks;
	unique_ptr<Expression> else_expr;
	bound_checks.reserve(expr.case_checks.size());

	for (auto &check : expr.case_checks) {
		BindChild(check.when_expr, depth, error);
		if (error.HasError()) {
			return BindResult(std::move(error));
		}

		auto &bound_when_expr = BoundExpression::GetExpression(*check.when_expr);
		auto when_expr = BoundCastExpression::AddCastToType(context, std::move(bound_when_expr), LogicalType::BOOLEAN);

		bool constant_when = false;
		bool when_true = false;
		if (when_expr->IsFoldable()) {
			try {
				auto condition = ExpressionExecutor::EvaluateScalar(context, *when_expr).DefaultCastAs(LogicalType::BOOLEAN);
				constant_when = true;
				when_true = !condition.IsNull() && BooleanValue::Get(condition);
			} catch (const Exception &) {
				constant_when = false;
			}
		}

		if (constant_when && !when_true) {
			continue;
		}

		BindChild(check.then_expr, depth, error);
		if (error.HasError()) {
			return BindResult(std::move(error));
		}
		auto &bound_then_expr = BoundExpression::GetExpression(*check.then_expr);

		if (constant_when && when_true) {
			else_expr = std::move(bound_then_expr);
			break;
		}

		BoundCaseCheck result_check;
		result_check.when_expr = std::move(when_expr);
		result_check.then_expr = std::move(bound_then_expr);
		bound_checks.push_back(std::move(result_check));
	}

	if (!else_expr) {
		BindChild(expr.else_expr, depth, error);
		if (error.HasError()) {
			return BindResult(std::move(error));
		}
		auto &bound_else_expr = BoundExpression::GetExpression(*expr.else_expr);
		else_expr = std::move(bound_else_expr);
	}

	if (error.HasError()) {
		return BindResult(std::move(error));
	}

	// Figure out the return type of the remaining CASE expression.
	auto return_type = ExpressionBinder::GetExpressionReturnType(*else_expr);
	for (auto &check : bound_checks) {
		auto then_type = ExpressionBinder::GetExpressionReturnType(*check.then_expr);
		if (!LogicalType::TryGetMaxLogicalType(context, return_type, then_type, return_type)) {
			throw BinderException(
			    expr, "Cannot mix values of type %s and %s in CASE expression - an explicit cast is required",
			    return_type.ToString(), then_type.ToString());
		}
	}

	if (bound_checks.empty()) {
		return BindResult(BoundCastExpression::AddCastToType(context, std::move(else_expr), return_type));
	}

	// Build the pruned bound CASE expression.
	auto result = make_uniq<BoundCaseExpression>(return_type);
	for (auto &check : bound_checks) {
		check.then_expr = BoundCastExpression::AddCastToType(context, std::move(check.then_expr), return_type);
		result->case_checks.push_back(std::move(check));
	}
	result->else_expr = BoundCastExpression::AddCastToType(context, std::move(else_expr), return_type);
	return BindResult(std::move(result));
}
} // namespace duckdb
