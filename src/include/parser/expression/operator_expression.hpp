//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/operator_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
//! Represents a built-in operator expression
class OperatorExpression : public AbstractExpression {
  public:
	OperatorExpression(ExpressionType type, TypeId type_id = TypeId::INVALID)
	    : AbstractExpression(type, type_id) {}
	OperatorExpression(ExpressionType type, TypeId type_id,
	                   std::unique_ptr<AbstractExpression> left,
	                   std::unique_ptr<AbstractExpression> right = nullptr)
	    : AbstractExpression(type, type_id, std::move(left), std::move(right)) {
	}

	virtual void ResolveType() override {
		AbstractExpression::ResolveType();
		// logical operators return a bool
		if (type == ExpressionType::OPERATOR_NOT ||
		    type == ExpressionType::OPERATOR_IS_NULL ||
		    type == ExpressionType::OPERATOR_IS_NOT_NULL ||
		    type == ExpressionType::OPERATOR_EXISTS ||
		    type == ExpressionType::COMPARE_IN) {
			return_type = TypeId::BOOLEAN;
			return;
		}
		switch (type) {
		case ExpressionType::OPERATOR_ADD:
			Statistics::Add(children[0]->stats, children[1]->stats, stats);
			break;
		case ExpressionType::OPERATOR_SUBTRACT:
			Statistics::Subtract(children[0]->stats, children[1]->stats, stats);
			break;
		case ExpressionType::OPERATOR_MULTIPLY:
			Statistics::Multiply(children[0]->stats, children[1]->stats, stats);
			break;
		case ExpressionType::OPERATOR_DIVIDE:
			Statistics::Divide(children[0]->stats, children[1]->stats, stats);
			break;
		case ExpressionType::OPERATOR_MOD:
			Statistics::Modulo(children[0]->stats, children[1]->stats, stats);
			break;
		default:
			throw NotImplementedException("Unsupported operator type!");
		}
		// return the highest type of the children, unless we need to upcast to
		// avoid overflow
		return_type =
		    std::max(children[0]->return_type, children[1]->return_type);
		if (!stats.FitsInType(return_type)) {
			return_type = stats.MinimalType();
		}
	}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
};
} // namespace duckdb
