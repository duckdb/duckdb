#include <duckdb/planner/expression/list.hpp>
#include "duckdb/optimizer/rule/comparison_simplification.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

using namespace duckdb;
using namespace std;

ComparisonSimplificationRule::ComparisonSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a ComparisonExpression that has a ConstantExpression as a check
	auto op = make_unique<ComparisonExpressionMatcher>();
	op->matchers.push_back(make_unique<FoldableConstantMatcher>());
	op->policy = SetMatcher::Policy::SOME;
	root = move(op);
}

unique_ptr<Expression> ComparisonSimplificationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                           bool &changes_made) {
	auto constant_expr = bindings[1];
	// the constant_expr is a scalar expression that we have to fold
	// use an ExpressionExecutor to execute the expression
	assert(constant_expr->IsFoldable());
	auto constant_value = ExpressionExecutor::EvaluateScalar(*constant_expr);
	if (constant_value.is_null) {
		// comparison with constant NULL, return NULL
		return make_unique<BoundConstantExpression>(Value(TypeId::BOOL));
	}
	if(constant_expr->type == ExpressionType::OPERATOR_CAST){
	    auto cast_expression = (BoundCastExpression*) constant_expr;
	    auto child_expression = (BoundConstantExpression*) cast_expression->child.get();
	    auto new_constant = child_expression->value.TryCastAs(cast_expression->source_type.id, cast_expression->target_type.id);
	    //! turn this into a bount constant expression
	    auto bound_const_expr = make_unique<BoundConstantExpression>(new_constant);
	    //! We can try to cast the constant
	    return bound_const_expr;
	}
	return nullptr;
}
