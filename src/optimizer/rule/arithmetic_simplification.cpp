#include "optimizer/rule/arithmetic_simplification.hpp"

#include "common/exception.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/expression/bound_operator_expression.hpp"

using namespace duckdb;
using namespace std;

ArithmeticSimplificationRule::ArithmeticSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on an OperatorExpression that has a ConstantExpression as child
	auto op = make_unique<OperatorExpressionMatcher>();
	op->matchers.push_back(make_unique<ConstantExpressionMatcher>());
	op->policy = SetMatcher::Policy::SOME;
	// we match only on arithmetic expressions
	vector<ExpressionType> arithmetic_types{ExpressionType::OPERATOR_ADD, ExpressionType::OPERATOR_SUBTRACT,
	                                        ExpressionType::OPERATOR_MULTIPLY, ExpressionType::OPERATOR_DIVIDE};
	op->expr_type = make_unique<ManyExpressionTypeMatcher>(arithmetic_types);
	// and only with numeric results
	op->type = make_unique<IntegerTypeMatcher>();
	root = move(op);
}

unique_ptr<Expression> ArithmeticSimplificationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                           bool &changes_made) {
	auto root = (BoundOperatorExpression *)bindings[0];
	auto constant = (BoundConstantExpression *)bindings[1];
	int constant_child = root->children[0].get() == constant ? 0 : 1;
	assert(root->children.size() == 2);
	// any arithmetic operator involving NULL is always NULL
	if (constant->value.is_null) {
		return make_unique<BoundConstantExpression>(Value(root->return_type));
	}
	switch (root->type) {
	case ExpressionType::OPERATOR_ADD:
		if (constant->value == 0) {
			// addition with 0
			// we can remove the entire operator and replace it with the non-constant child
			return move(root->children[1 - constant_child]);
		}
		break;
	case ExpressionType::OPERATOR_SUBTRACT:
		if (constant_child == 1 && constant->value == 0) {
			// subtraction by 0
			// we can remove the entire operator and replace it with the non-constant child
			return move(root->children[1 - constant_child]);
		}
		break;
	case ExpressionType::OPERATOR_MULTIPLY:
		if (constant->value == 1) {
			// multiply with 1, replace with non-constant child
			return move(root->children[1 - constant_child]);
		}
		break;
	default:
		assert(root->type == ExpressionType::OPERATOR_DIVIDE);
		if (constant_child == 1) {
			if (constant->value == 1) {
				// divide by 1, replace with non-constant child
				return move(root->children[1 - constant_child]);
			} else if (constant->value == 0) {
				// divide by 0, replace with NULL
				return make_unique<BoundConstantExpression>(Value(root->return_type));
			}
		}
		break;
	}
	return nullptr;
}
