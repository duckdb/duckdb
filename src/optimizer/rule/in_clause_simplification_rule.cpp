
#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/optimizer/rule/in_clause_simplification.hpp"
#include "duckdb/planner/expression/list.hpp"
#include "duckdb/planner/expression/bound_operator_expression.hpp"

namespace duckdb {

InClauseSimplificationRule::InClauseSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on InClauseExpression that has a ConstantExpression as a check
	auto op = make_uniq<InClauseExpressionMatcher>();
	op->policy = SetMatcher::Policy::SOME;
	root = std::move(op);
}

unique_ptr<Expression> InClauseSimplificationRule::Apply(LogicalOperator &op, vector<reference<Expression>> &bindings,
                                                         bool &changes_made, bool is_root) {
	auto &expr = bindings[0].get().Cast<BoundOperatorExpression>();
	if (expr.children[0]->expression_class != ExpressionClass::BOUND_CAST) {
		return nullptr;
	}
	auto &cast_expression = expr.children[0]->Cast<BoundCastExpression>();
	if (cast_expression.child->expression_class != ExpressionClass::BOUND_COLUMN_REF) {
		return nullptr;
	}
	//! The goal here is to remove the cast from the probe expression
	//! and apply a cast to the constant expressions. We can only do this
	//! if the semantics do not change, which only happens when BOTH casts
	//! are invertible.
	auto target_type = cast_expression.source_type();
	if (!BoundCastExpression::CastIsInvertible(target_type, cast_expression.return_type)) {
		return nullptr;
	}
	vector<unique_ptr<BoundConstantExpression>> cast_list;
	//! First check if we can cast all children
	for (size_t i = 1; i < expr.children.size(); i++) {
		if (expr.children[i]->expression_class != ExpressionClass::BOUND_CONSTANT) {
			return nullptr;
		}
		D_ASSERT(expr.children[i]->IsFoldable());
		auto constant_value = ExpressionExecutor::EvaluateScalar(GetContext(), *expr.children[i]);
		if (!BoundCastExpression::CastIsInvertible(constant_value.type(), target_type)) {
			return nullptr;
		}
		auto new_constant = constant_value.DefaultTryCastAs(target_type);
		if (!new_constant) {
			return nullptr;
		} else {
			auto new_constant_expr = make_uniq<BoundConstantExpression>(constant_value);
			cast_list.push_back(std::move(new_constant_expr));
		}
	}
	//! We can cast, so we move the new constant
	for (size_t i = 1; i < expr.children.size(); i++) {
		expr.children[i] = std::move(cast_list[i - 1]);

		//		expr->children[i] = std::move(new_constant_expr);
	}
	//! We can cast the full list, so we move the column
	expr.children[0] = std::move(cast_expression.child);
	return nullptr;
}

} // namespace duckdb
