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

#include "optimizer/rule.hpp"
#include "parser/expression/constant_expression.hpp"

namespace duckdb {

class ConstantFoldingRule : public OptimizerRule {
  public:
	ConstantFoldingRule() {
		root = OptimizerNode(ExpressionType::OPERATOR_ADD);
		root.children = {OptimizerNode(ExpressionType::VALUE_CONSTANT),
		                 OptimizerNode(ExpressionType::VALUE_CONSTANT)};
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
