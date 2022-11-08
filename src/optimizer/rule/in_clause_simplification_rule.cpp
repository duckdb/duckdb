#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/rule/in_clause_simplification.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

InClauseSimplificationRule::InClauseSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on InClauseExpression that has a ConstantExpression as a check
	auto op = make_unique<InClauseExpressionMatcher>();
	op->policy = SetMatcher::Policy::SOME;
	root = move(op);
}

unique_ptr<Expression> InClauseSimplificationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                         bool &changes_made, bool is_root) {
	D_ASSERT(bindings[0]->expression_class == ExpressionClass::BOUND_OPERATOR);
	auto expr = (BoundOperatorExpression *)bindings[0];
	if (expr->children[0]->expression_class != ExpressionClass::BOUND_CAST) {
		return nullptr;
	}
	auto cast_expression = (BoundCastExpression *)expr->children[0].get();
	if (cast_expression->child->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	//! Here we check if we can apply the expression on the constant side
	auto target_type = cast_expression->source_type();
	if (!BoundCastExpression::CastIsInvertible(cast_expression->return_type, target_type)) {
		return nullptr;
	}
	vector<unique_ptr<BoundConstantExpression>> cast_list;
	//! First check if we can cast all children
	for (size_t i = 1; i < expr->children.size(); i++) {
		if (expr->children[i]->expression_class != ExpressionClass::BOUND_CONSTANT) {
			return nullptr;
		}
		D_ASSERT(expr->children[i]->IsFoldable());
		auto constant_value = ExpressionExecutor::EvaluateScalar(rewriter.context, *expr->children[i]);
		auto new_constant = constant_value.DefaultTryCastAs(target_type);
		if (!new_constant) {
			return nullptr;
		} else {
			auto new_constant_expr = make_unique<BoundConstantExpression>(constant_value);
			cast_list.push_back(move(new_constant_expr));
		}
	}
	//! We can cast, so we move the new constant
	for (size_t i = 1; i < expr->children.size(); i++) {
		expr->children[i] = move(cast_list[i - 1]);

		//		expr->children[i] = move(new_constant_expr);
	}
	//! We can cast the full list, so we move the column
	expr->children[0] = move(cast_expression->child);
	return nullptr;
}

} // namespace duckdb
