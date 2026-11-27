#include "duckdb/optimizer/rule/case_simplification.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"

#include "duckdb/planner/expression_iterator.hpp"

namespace duckdb {

static void SubstituteOperand(unique_ptr<Expression> &expr, const Expression &value) {
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_CASE_OPERAND) {
		expr = value.Copy();
		return;
	}
	if (expr->GetExpressionClass() == ExpressionClass::BOUND_CASE && expr->Cast<BoundCaseExpression>().Operand()) {
		return;
	}
	ExpressionIterator::EnumerateChildren(*expr,
	                                      [&](unique_ptr<Expression> &child) { SubstituteOperand(child, value); });
}

CaseSimplificationRule::CaseSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a CaseExpression that has a ConstantExpression as a check
	auto op = make_uniq<CaseExpressionMatcher>();
	root = std::move(op);
}

unique_ptr<Expression> CaseSimplificationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                     bool &changes_made, bool is_root) {
	auto &root = bindings[0].get().Cast<BoundCaseExpression>();
	// A planned scalar subquery already has one result column shared by all its uses.
	if (root.Operand() && root.Operand()->GetExpressionClass() == ExpressionClass::BOUND_COLUMN_REF) {
		for (auto &check : root.CaseChecksMutable()) {
			SubstituteOperand(check.when_expr, *root.Operand());
		}
		root.OperandMutable().reset();
		changes_made = true;
	}
	for (idx_t i = 0; i < root.CaseChecks().size(); i++) {
		auto &case_check = root.CaseChecksMutable()[i];
		if (case_check.when_expr->IsFoldable()) {
			// the WHEN check is a foldable expression
			// use an ExpressionExecutor to execute the expression
			Value condition;
			if (!ExpressionExecutor::TryEvaluateScalar(GetContext(), *case_check.when_expr, condition) ||
			    !condition.TryCastAs(GetContext(), LogicalType::BOOLEAN)) {
				// the condition cannot be evaluated: leave it to execution
				continue;
			}

			// fold based on the constant condition
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
				                               root.CaseChecks().end());
				break;
			}
		}
	}
	if (root.CaseChecks().empty() && !root.Operand()) {
		// no case checks left: return the ELSE expression
		return Expression::PreserveReturnType(root.GetReturnType(), std::move(root.ElseMutable()));
	}
	return nullptr;
}

} // namespace duckdb
