//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/parser.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <vector>

#include "optimizer/rule.hpp"
#include "parser/expression/constant_expression.hpp"

namespace duckdb {

class OptimizerNodeExpressionSet : public OptimizerNode {
  public:
	std::vector<ExpressionType> types;
	OptimizerNodeExpressionSet(std::vector<ExpressionType> types)
	    : types(types) {}
	virtual bool Matches(AbstractExpression &rel) {
		return std::find(types.begin(), types.end(), rel.type) != types.end();
	}
};

class ConstantFoldingRule : public OptimizerRule {
  public:
	ConstantFoldingRule() {
		root = std::unique_ptr<OptimizerNode>(new OptimizerNodeExpressionSet(
		    {ExpressionType::OPERATOR_ADD,
		     ExpressionType::OPERATOR_MINUS})); // TODO: more
		root->children.push_back(std::unique_ptr<OptimizerNode>(
		    new OptimizerNodeExpressionSet({ExpressionType::VALUE_CONSTANT})));
		root->children.push_back(std::unique_ptr<OptimizerNode>(
		    new OptimizerNodeExpressionSet({ExpressionType::VALUE_CONSTANT})));
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
		Value::Add(left->value, right->value, result);
		return make_unique<ConstantExpression>(result);
	};
};

} // namespace duckdb
