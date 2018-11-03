//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/conjunction_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression.hpp"

namespace duckdb {
//! Represents a conjunction (AND/OR)
class ConjunctionExpression : public Expression {
  public:
	ConjunctionExpression(ExpressionType type, std::unique_ptr<Expression> left,
	                      std::unique_ptr<Expression> right)
	    : Expression(type, TypeId::BOOLEAN, std::move(left), std::move(right)) {
	}

	virtual void Accept(SQLNodeVisitor *v) override {
		v->Visit(*this);
	}
	virtual ExpressionClass GetExpressionClass() override {
		return ExpressionClass::CONJUNCTION;
	}

	virtual std::unique_ptr<Expression> Copy() override;

	bool Equals(const Expression *other) override;

	//! Deserializes a blob back into a ConjunctionExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);
};
} // namespace duckdb
