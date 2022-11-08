#include "duckdb/optimizer/rule/conjunction_simplification.hpp"

#include "duckdb/execution/expression_executor.hpp"
#include "duckdb/planner/expression/bound_conjunction_expression.hpp"
#include "duckdb/planner/expression/bound_constant_expression.hpp"

namespace duckdb {

ConjunctionSimplificationRule::ConjunctionSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on a ComparisonExpression that has a ConstantExpression as a check
	auto op = make_unique<ConjunctionExpressionMatcher>();
	op->matchers.push_back(make_unique<FoldableConstantMatcher>());
	op->policy = SetMatcher::Policy::SOME;
	root = move(op);
}

unique_ptr<Expression> ConjunctionSimplificationRule::RemoveExpression(BoundConjunctionExpression &conj,
                                                                       Expression *expr) {
	for (idx_t i = 0; i < conj.children.size(); i++) {
		if (conj.children[i].get() == expr) {
			// erase the expression
			conj.children.erase(conj.children.begin() + i);
			break;
		}
	}
	if (conj.children.size() == 1) {
		// one expression remaining: simply return that expression and erase the conjunction
		return move(conj.children[0]);
	}
	return nullptr;
}

unique_ptr<Expression> ConjunctionSimplificationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                            bool &changes_made, bool is_root) {
	auto conjunction = (BoundConjunctionExpression *)bindings[0];
	auto constant_expr = bindings[1];
	// the constant_expr is a scalar expression that we have to fold
	// use an ExpressionExecutor to execute the expression
	D_ASSERT(constant_expr->IsFoldable());
	Value constant_value;
	if (!ExpressionExecutor::TryEvaluateScalar(rewriter.context, *constant_expr, constant_value)) {
		return nullptr;
	}
	constant_value = constant_value.DefaultCastAs(LogicalType::BOOLEAN);
	if (constant_value.IsNull()) {
		// we can't simplify conjunctions with a constant NULL
		return nullptr;
	}
	if (conjunction->type == ExpressionType::CONJUNCTION_AND) {
		if (!BooleanValue::Get(constant_value)) {
			// FALSE in AND, result of expression is false
			return make_unique<BoundConstantExpression>(Value::BOOLEAN(false));
		} else {
			// TRUE in AND, remove the expression from the set
			return RemoveExpression(*conjunction, constant_expr);
		}
	} else {
		D_ASSERT(conjunction->type == ExpressionType::CONJUNCTION_OR);
		if (!BooleanValue::Get(constant_value)) {
			// FALSE in OR, remove the expression from the set
			return RemoveExpression(*conjunction, constant_expr);
		} else {
			// TRUE in OR, result of expression is true
			return make_unique<BoundConstantExpression>(Value::BOOLEAN(true));
		}
	}
}

} // namespace duckdb
