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
#include "common/internal_types.hpp"
#include "optimizer/rule.hpp"
#include "parser/expression/constant_expression.hpp"

namespace duckdb {

class ConstantFoldingRule : public OptimizerRule {
  public:
	ConstantFoldingRule() {
		root = std::unique_ptr<OptimizerNode>(new OptimizerNodeExpressionSet(
		    {ExpressionType::OPERATOR_ADD, ExpressionType::OPERATOR_SUBTRACT,
		     ExpressionType::OPERATOR_MULTIPLY, ExpressionType::OPERATOR_DIVIDE,
		     ExpressionType::OPERATOR_MOD}));
		root->children.push_back(std::unique_ptr<OptimizerNode>(
		    new OptimizerNodeExpression(ExpressionType::VALUE_CONSTANT)));
		root->children.push_back(
		    std::unique_ptr<OptimizerNode>(new OptimizerNodeAny()));
		root->child_policy = ChildPolicy::UNORDERED;
	}

	std::unique_ptr<AbstractExpression>
	Apply(AbstractExpression &root,
	      std::vector<AbstractExpression *> &bindings) {
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

		Value zero = Value(0);
		Value one = Value(1);

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
					if (Value::Equals(right_val->value, zero)) {
						return make_unique<ConstantExpression>(NullValue<bool>);
					}
					if (Value::Equals(right_val->value, one)) {
						return move(root.children[0]);
					}
					break;
				case ExpressionType::OPERATOR_MOD:
					if (Value::Equals(right_val->value, zero)) {
						return make_unique<ConstantExpression>(NullValue<bool>);
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
				case ExpressionType::OPERATOR_ADD:
					if (Value::Equals(left_val->value, zero)) {
						return move(root.children[1]);
					}
					break;
				case ExpressionType::OPERATOR_MULTIPLY:
					if (Value::Equals(left_val->value, zero)) {
						return make_unique<ConstantExpression>(zero);
					}
					if (Value::Equals(left_val->value, one)) {
						return move(root.children[1]);
					}
					break;
				case ExpressionType::OPERATOR_DIVIDE:
					if (Value::Equals(left_val->value, zero)) {
						return make_unique<ConstantExpression>(zero);
					}
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
