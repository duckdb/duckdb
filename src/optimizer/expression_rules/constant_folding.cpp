#include "optimizer/expression_rules/constant_folding.hpp"

#include "common/common.hpp"
#include "common/exception.hpp"
#include "common/value_operations/value_operations.hpp"
#include "optimizer/rule.hpp"
#include "parser/expression/constant_expression.hpp"

#include <algorithm>
#include <vector>

using namespace duckdb;
using namespace std;

ConstantFoldingRule::ConstantFoldingRule() {
	root = unique_ptr<AbstractRuleNode>(new ExpressionNodeSet(
	    {ExpressionType::OPERATOR_ADD, ExpressionType::OPERATOR_SUBTRACT, ExpressionType::OPERATOR_MULTIPLY,
	     ExpressionType::OPERATOR_DIVIDE, ExpressionType::OPERATOR_MOD}));
	root->children.push_back(make_unique_base<AbstractRuleNode, ExpressionNodeType>(ExpressionType::VALUE_CONSTANT));
	root->children.push_back(make_unique_base<AbstractRuleNode, ExpressionNodeAny>());
	root->child_policy = ChildPolicy::UNORDERED;
}

unique_ptr<Expression> ConstantFoldingRule::Apply(Rewriter &rewriter, Expression &root,
                                                  vector<AbstractOperator> &bindings, bool &fixed_point) {
	Value result;

	// TODO: add boolean ops

	auto left = root.children[0].get();
	auto right = root.children[1].get();

	// case: both constant, evaluate
	if (left->type == ExpressionType::VALUE_CONSTANT && right->type == ExpressionType::VALUE_CONSTANT) {
		Value result;
		auto left_val = reinterpret_cast<ConstantExpression *>(root.children[0].get());
		auto right_val = reinterpret_cast<ConstantExpression *>(root.children[1].get());

		if (TypeIsNumeric(left_val->value.type) && TypeIsNumeric(right_val->value.type)) {
			switch (root.type) {
			case ExpressionType::OPERATOR_ADD:
				result = left_val->value + right_val->value;
				break;
			case ExpressionType::OPERATOR_SUBTRACT:
				result = left_val->value - right_val->value;
				break;
			case ExpressionType::OPERATOR_MULTIPLY:
				result = left_val->value * right_val->value;
				break;
			case ExpressionType::OPERATOR_DIVIDE:
				result = left_val->value / right_val->value;
				break;
			case ExpressionType::OPERATOR_MOD:
				// Value::Modulo(left_val->value, right_val->value, result);
				// break;
			default:
				throw NotImplementedException("Unsupported operator");
			}
			return make_unique<ConstantExpression>(result.CastAs(root.return_type));
		}
		return nullptr;
	}

	// FIXME:  folding unknown subtrees produces incorrect results with
	// NULLs.
	return nullptr;
};
