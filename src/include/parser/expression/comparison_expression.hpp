//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/comparison_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {
//! Represents a boolean comparison (e.g. =, >=, <>). Always returns a boolean
//! and has two children.
class ComparisonExpression : public Expression {
  public:
	ComparisonExpression(ExpressionType type, std::unique_ptr<Expression> left,
	                     std::unique_ptr<Expression> right)
	    : Expression(type, TypeId::BOOLEAN, std::move(left), std::move(right)) {
	}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual ExpressionClass GetExpressionClass() override {
		return ExpressionClass::COMPARISON;
	}

	//! Deserializes a blob back into an OperatorExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);
};
} // namespace duckdb
