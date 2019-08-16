#include "optimizer/rule/arithmetic_simplification.hpp"

#include "common/exception.hpp"
#include "planner/expression/bound_constant_expression.hpp"
#include "planner/expression/bound_function_expression.hpp"

using namespace duckdb;
using namespace std;

ArithmeticSimplificationRule::ArithmeticSimplificationRule(ExpressionRewriter &rewriter) : Rule(rewriter) {
	// match on an OperatorExpression that has a ConstantExpression as child
	auto op = make_unique<FunctionExpressionMatcher>();
	op->matchers.push_back(make_unique<ConstantExpressionMatcher>());
	op->policy = SetMatcher::Policy::SOME;
	// we only match on simple arithmetic expressions (+, -, *, /)
	op->op_matcher = make_unique<ManyOperatorTypeMatcher>(vector<OperatorType>{OperatorType::ADD, OperatorType::SUBTRACT, OperatorType::MULTIPLY, OperatorType::DIVIDE});
	// and only with numeric results
	op->type = make_unique<IntegerTypeMatcher>();
	root = move(op);
}

unique_ptr<Expression> ArithmeticSimplificationRule::Apply(LogicalOperator &op, vector<Expression *> &bindings,
                                                           bool &changes_made) {
	auto root = (BoundFunctionExpression *)bindings[0];
	auto constant = (BoundConstantExpression *)bindings[1];
	int constant_child = root->children[0].get() == constant ? 0 : 1;
	assert(root->children.size() == 2);
	// any arithmetic operator involving NULL is always NULL
	if (constant->value.is_null) {
		return make_unique<BoundConstantExpression>(Value(root->return_type));
	}
	switch (root->op_type) {
	case OperatorType::ADD:
		if (constant->value == 0) {
			// addition with 0
			// we can remove the entire operator and replace it with the non-constant child
			return move(root->children[1 - constant_child]);
		}
		break;
	case OperatorType::SUBTRACT:
		if (constant_child == 1 && constant->value == 0) {
			// subtraction by 0
			// we can remove the entire operator and replace it with the non-constant child
			return move(root->children[1 - constant_child]);
		}
		break;
	case OperatorType::MULTIPLY:
		if (constant->value == 1) {
			// multiply with 1, replace with non-constant child
			return move(root->children[1 - constant_child]);
		}
		break;
	default:
		assert(root->op_type == OperatorType::DIVIDE);
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
