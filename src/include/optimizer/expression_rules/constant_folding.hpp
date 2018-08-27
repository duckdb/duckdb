//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// optimizer/expression_rules/constant_folding.hpp
//
// Author: Hannes Mühleisen & Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <algorithm>
#include <vector>

#include "common/exception.hpp"
#include "common/internal_types.hpp"
#include "optimizer/rule.hpp"
#include "parser/expression/constant_expression.hpp"

namespace duckdb {

class ConstantFoldingRule : public Rule {
  public:
	ConstantFoldingRule() {
		root = std::unique_ptr<AbstractRuleNode>(new ExpressionNodeSet(
		    {ExpressionType::OPERATOR_ADD, ExpressionType::OPERATOR_SUBTRACT,
		     ExpressionType::OPERATOR_MULTIPLY, ExpressionType::OPERATOR_DIVIDE,
		     ExpressionType::OPERATOR_MOD}));
		root->children.push_back(
		    make_unique_base<AbstractRuleNode, ExpressionNodeType>(
		        ExpressionType::VALUE_CONSTANT));
		root->children.push_back(
		    make_unique_base<AbstractRuleNode, ExpressionNodeAny>());
		root->child_policy = ChildPolicy::UNORDERED;
	}

	std::unique_ptr<AbstractExpression>
	Apply(AbstractExpression &root, std::vector<AbstractOperator> &bindings) {
		Value result;

		// TODO: add bolean ops

		auto left = root.children[0].get();
		auto right = root.children[1].get();
		Value null = Value();

		// case: both constant, evaluate
		if (left->type == ExpressionType::VALUE_CONSTANT &&
		    right->type == ExpressionType::VALUE_CONSTANT) {
			Value result;
			auto left_val =
			    reinterpret_cast<ConstantExpression *>(root.children[0].get());
			auto right_val =
			    reinterpret_cast<ConstantExpression *>(root.children[1].get());

			if (TypeIsNumeric(left_val->value.type) &&
			    TypeIsNumeric(right_val->value.type)) {
				switch (root.type) {
				case ExpressionType::OPERATOR_ADD:
					Value::Add(left_val->value, right_val->value, result);
					break;
				case ExpressionType::OPERATOR_SUBTRACT:
					Value::Subtract(left_val->value, right_val->value, result);
					break;
				case ExpressionType::OPERATOR_MULTIPLY:
					Value::Multiply(left_val->value, right_val->value, result);
					break;
				case ExpressionType::OPERATOR_DIVIDE:
					Value::Divide(left_val->value, right_val->value, result);
					break;
				case ExpressionType::OPERATOR_MOD:
					Value::Modulo(left_val->value, right_val->value, result);
					break;
				default:
					throw Exception("Unsupported operator");
				}
				return make_unique<ConstantExpression>(result);
			}
			return nullptr;
		}

		// FIXME:  folding unknown subtrees produces incorrect results with
		// NULLs.
		return nullptr;
	};
};

} // namespace duckdb
