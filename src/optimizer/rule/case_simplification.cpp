#include "duckdb/optimizer/rule/case_simplification.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

static bool IsBooleanConstant(const Expression &expr, bool expected_value) {
	if (expr.GetExpressionClass() != ExpressionClass::BOUND_CONSTANT) {
		return false;
	}

	const auto &constant = expr.Cast<BoundConstantExpression>().GetValue();
	return !constant.IsNull() && constant.type() == LogicalType::BOOLEAN &&
	       BooleanValue::Get(constant) == expected_value;
}

CaseSimplificationRule::CaseSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a CaseExpression that has a ConstantExpression as a check
	auto op = make_uniq<CaseExpressionMatcher>();
	root = std::move(op);
}

unique_ptr<Expression> CaseSimplificationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                     bool &changes_made, bool is_root) {
	auto &root = bindings[0].get().Cast<BoundCaseExpression>();
	for (idx_t i = 0; i < root.CaseChecksMutable().size(); i++) {
		auto &case_check = root.CaseChecksMutable()[i];
		if (case_check.when_expr->IsFoldable()) {
			// the WHEN check is a foldable expression
			// use an ExpressionExecutor to execute the expression
			auto constant_value = ExpressionExecutor::EvaluateScalar(GetContext(), *case_check.when_expr);

			// fold based on the constant condition
			auto condition = constant_value.DefaultCastAs(LogicalType::BOOLEAN);
			if (condition.IsNull() || !BooleanValue::Get(condition)) {
				// the condition is always false: remove this case check
				root.CaseChecksMutable().erase_at(i);
				i--;
			} else {
				// the condition is always true
				// move the THEN clause to the ELSE of the case
				root.ElseMutable() = std::move(case_check.then_expr);
				// remove this case check and any case checks after this one
				root.CaseChecksMutable().erase(root.CaseChecksMutable().begin() + NumericCast<int64_t>(i),
				                               root.CaseChecksMutable().end());
				break;
			}
		}
	}
	if (root.CaseChecksMutable().empty()) {
		// no case checks left: return the ELSE expression
		return std::move(root.ElseMutable());
	}

	// This rewrite is only valid at a filter root, where FALSE and NULL both reject the row.
	if (!is_root || op.type != LogicalOperatorType::LOGICAL_FILTER) {
		return nullptr;
	}

	if (root.CaseChecks().size() != 1) {
		return nullptr;
	}

	auto &case_check = root.CaseChecksMutable()[0];
	if (!IsBooleanConstant(*case_check.then_expr, true) || !IsBooleanConstant(root.Else(), false)) {
		return nullptr;
	}

	// CASE WHEN predicate THEN true ELSE false END -> predicate
	return std::move(case_check.when_expr);
}

} // namespace duckdb
