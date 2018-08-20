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
		// TODO: fix bindings, they could be used here

		auto left = root.children[0].get();
		auto right = root.children[1].get();

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

		Value zero = Value::BIGINT(0);
		Value one = Value::BIGINT(1);

		// case: right is constant
		if (right->type == ExpressionType::VALUE_CONSTANT) {
			auto right_val = reinterpret_cast<ConstantExpression *>(right);
			if (TypeIsNumeric(right_val->value.type)) {
				switch (root.type) {
				case ExpressionType::OPERATOR_ADD:
				case ExpressionType::OPERATOR_SUBTRACT:
					if (Value::Equals(right_val->value, zero)) {
						return move(root.children[0]);
					}
					break;
				case ExpressionType::OPERATOR_MULTIPLY:
					if (Value::Equals(right_val->value, zero)) {
						return make_unique<ConstantExpression>(zero);
					}
					if (Value::Equals(right_val->value, one)) {
						return move(root.children[0]);
					}
					break;
				case ExpressionType::OPERATOR_DIVIDE:
					if (Value::Equals(right_val->value, zero)) { // X / 0 = NULL
						return make_unique<ConstantExpression>(Value());
					}
					if (Value::Equals(right_val->value, one)) { // X / 1 == X
						return move(root.children[0]);
					}
					break;
				case ExpressionType::OPERATOR_MOD:
					if (Value::Equals(right_val->value,
					                  zero)) { // X % 0 == NULL
						return make_unique<ConstantExpression>(Value());
					}
					if (Value::Equals(right_val->value, one)) {
						return make_unique<ConstantExpression>(zero);
					}
					break;
				default:
					throw Exception("Unsupported operator");
				}
			}
		}

		// case: right is constant
		if (left->type == ExpressionType::VALUE_CONSTANT) {
			auto left_val = reinterpret_cast<ConstantExpression *>(left);
			if (TypeIsNumeric(left_val->value.type)) {
				switch (root.type) {
				case ExpressionType::OPERATOR_ADD: // X + 0 == X
					if (Value::Equals(left_val->value, zero)) {
						return move(root.children[1]);
					}
					break;
				case ExpressionType::OPERATOR_MULTIPLY:
					if (Value::Equals(left_val->value, zero)) { // X * 0 == 0
						return make_unique<ConstantExpression>(zero);
					}
					if (Value::Equals(left_val->value, one)) { // X * 1 = X
						return move(root.children[1]);
					}
					break;
				case ExpressionType::OPERATOR_DIVIDE: // 0 / X == 0
					if (Value::Equals(left_val->value, zero)) {
						return make_unique<ConstantExpression>(zero);
					}
					break;
				case ExpressionType::OPERATOR_MOD:
				case ExpressionType::OPERATOR_SUBTRACT:
					break;
				default:
					throw Exception("Unsupported operator");
				}
			}
		}

		return nullptr;
	};
};

} // namespace duckdb
