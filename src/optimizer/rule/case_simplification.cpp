#include "duckdb/optimizer/rule/case_simplification.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"

namespace duckdb {

CaseSimplificationRule::CaseSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a CaseExpression that has a ConstantExpression as a check
	auto op = make_uniq<CaseExpressionMatcher>();
	root = std::move(op);
}

unique_ptr<Expression> CaseSimplificationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                     bool &changes_made, bool is_root) {
	auto &root = bindings[0].get().Cast<BoundCaseExpression>();
	for (idx_t i = 0; i < root.case_checks.size(); i++) {
		auto &case_check = root.case_checks[i];
		if (case_check.when_expr->IsFoldable()) {
			// the WHEN check is a foldable expression
			// use an ExpressionExecutor to execute the expression
			auto constant_value = ExpressionExecutor::EvaluateScalar(GetContext(), *case_check.when_expr);

			// fold based on the constant condition
			auto condition = constant_value.DefaultCastAs(LogicalType::BOOLEAN);
			if (condition.IsNull() || !BooleanValue::Get(condition)) {
				// the condition is always false: remove this case check
				root.case_checks.erase(root.case_checks.begin() + i);
				i--;
			} else {
				// the condition is always true
				// move the THEN clause to the ELSE of the case
				root.else_expr = std::move(case_check.then_expr);
				// remove this case check and any case checks after this one
				root.case_checks.erase(root.case_checks.begin() + i, root.case_checks.end());
				break;
			}
		}
	}
	if (root.case_checks.empty()) {
		// no case checks left: return the ELSE expression
		return std::move(root.else_expr);
	}
	return nullptr;
}

} // namespace duckdb
