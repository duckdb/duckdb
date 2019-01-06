#include "optimizer/rule/constant_folding.hpp"

#include "common/common.hpp"
#include "common/exception.hpp"
#include "common/value_operations/value_operations.hpp"
#include "optimizer/rule.hpp"
#include "parser/expression/constant_expression.hpp"

using namespace duckdb;
using namespace std;

ConstantFoldingRule::ConstantFoldingRule() {
	// match an operator that involves two constants
	auto op = make_unique<OperatorExpressionMatcher>();
	op->matchers.push_back(make_unique<ConstantExpressionMatcher>());
	op->matchers.push_back(make_unique<ConstantExpressionMatcher>());
	op->policy = SetMatcher::Policy::ORDERED;
	// we match the following operators for constant folding
	vector<ExpressionType> supported_operations = {
	    ExpressionType::OPERATOR_ADD,
	    ExpressionType::OPERATOR_SUBTRACT,
	    ExpressionType::OPERATOR_MULTIPLY,
	    ExpressionType::OPERATOR_DIVIDE,
	    ExpressionType::OPERATOR_MOD};
	op->expr_type = make_unique<ManyExpressionTypeMatcher>(supported_operations);
	// for now we only support folding numeric constants
	op->matchers[0]->type = make_unique<NumericTypeMatcher>();
	op->matchers[1]->type = make_unique<NumericTypeMatcher>();
	
	root = move(op);
}

unique_ptr<Expression> ConstantFoldingRule::Apply(LogicalOperator &op, vector<Expression*> &bindings, bool &changes_made) {
	auto root = bindings[0];
	auto left = (ConstantExpression*) bindings[1];
	auto right = (ConstantExpression*) bindings[2];

	assert(TypeIsNumeric(left->return_type) && TypeIsNumeric(right->return_type));

	Value result;
	switch (root->type) {
	case ExpressionType::OPERATOR_ADD:
		result = left->value + right->value;
		break;
	case ExpressionType::OPERATOR_SUBTRACT:
		result = left->value - right->value;
		break;
	case ExpressionType::OPERATOR_MULTIPLY:
		result = left->value * right->value;
		break;
	case ExpressionType::OPERATOR_DIVIDE:
		result = left->value / right->value;
		break;
	case ExpressionType::OPERATOR_MOD:
		return nullptr;
	default:
		throw NotImplementedException("Unsupported operator");
	}
	// create a new ConstantExpression that replaces the root
	return make_unique<ConstantExpression>(result.CastAs(root->return_type));
}
