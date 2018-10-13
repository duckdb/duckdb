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

#include "parser/expression.hpp"

namespace duckdb {
//! Represents a built-in operator expression
class OperatorExpression : public Expression {
  public:
	OperatorExpression(ExpressionType type, TypeId type_id = TypeId::INVALID)
	    : Expression(type, type_id) {}
	OperatorExpression(ExpressionType type, TypeId type_id,
	                   std::unique_ptr<Expression> left,
	                   std::unique_ptr<Expression> right = nullptr)
	    : Expression(type, type_id, std::move(left), std::move(right)) {}

	virtual void ResolveType() override;

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual ExpressionClass GetExpressionClass() override {
		return ExpressionClass::OPERATOR;
	}

	//! Deserializes a blob back into an OperatorExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);
};
} // namespace duckdb
