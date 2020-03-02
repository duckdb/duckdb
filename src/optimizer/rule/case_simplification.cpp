#include "duckdb/optimizer/rule/case_simplification.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_case_expression.hpp"

using namespace duckdb;
using namespace std;

CaseSimplificationRule::CaseSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a CaseExpression that has a ConstantExpression as a check
	auto op = make_unique<CaseExpressionMatcher>();
	op->check = make_unique<FoldableConstantMatcher>();
	root = move(op);
}

unique_ptr<Expression> CaseSimplificationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                     bool &changes_made) {
	auto root = (BoundCaseExpression *)bindings[0];
	auto constant_expr = bindings[1];
	// the constant_expr is a scalar expression that we have to fold
	assert(constant_expr->IsFoldable());

	// use an ExpressionExecutor to execute the expression
	auto constant_value = ExpressionExecutor::EvaluateScalar(*constant_expr);

	// fold based on the constant condition
	auto condition = constant_value.CastAs(TypeId::BOOL);
	if (condition.is_null || !condition.value_.boolean) {
		return move(root->result_if_false);
	} else {
		return move(root->result_if_true);
	}
}
