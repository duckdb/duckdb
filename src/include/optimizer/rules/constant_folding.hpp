//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/parser.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <vector>

#include "common/exception.hpp"
#include "optimizer/rule.hpp"
#include "parser/expression/constant_expression.hpp"

namespace duckdb {

class ConstantFoldingRule : public OptimizerRule {
  public:
	ConstantFoldingRule() {
		root = std::unique_ptr<OptimizerNode>(new OptimizerNodeExpressionSet(
		    {ExpressionType::OPERATOR_ADD, ExpressionType::OPERATOR_SUBTRACT,
		     ExpressionType::OPERATOR_MULTIPLY, ExpressionType::OPERATOR_DIVIDE,
		     ExpressionType::OPERATOR_MOD})); // TODO: more?
		root->children.push_back(std::unique_ptr<OptimizerNode>(
		    new OptimizerNodeExpression(ExpressionType::VALUE_CONSTANT)));
		root->children.push_back(std::unique_ptr<OptimizerNode>(
		    new OptimizerNodeExpression(ExpressionType::VALUE_CONSTANT)));
		root->child_policy = ChildPolicy::UNORDERED;
	}

	std::unique_ptr<AbstractExpression>
	Apply(AbstractExpression &root,
	      std::vector<AbstractExpression *> &bindings) {
		Value result;
		auto left =
		    reinterpret_cast<ConstantExpression *>(root.children[0].get());
		auto right =
		    reinterpret_cast<ConstantExpression *>(root.children[1].get());

		if (left->value.type != TypeId::INTEGER ||
		    right->value.type != TypeId::INTEGER) {
			return nullptr;
		}
		switch (root.type) {
		case ExpressionType::OPERATOR_ADD:
			Value::Add(left->value, right->value, result);
			break;
		case ExpressionType::OPERATOR_SUBTRACT:
			Value::Subtract(left->value, right->value, result);
			break;
		case ExpressionType::OPERATOR_MULTIPLY:
			Value::Multiply(left->value, right->value, result);
			break;
		case ExpressionType::OPERATOR_DIVIDE:
			Value::Divide(left->value, right->value, result);
			break;
		case ExpressionType::OPERATOR_MOD:
			Value::Modulo(left->value, right->value, result);
			break;
		default:
			throw Exception("Unsupported operator");
		}

		return make_unique<ConstantExpression>(result);
	};
};

} // namespace duckdb
