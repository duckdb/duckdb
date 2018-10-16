//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/constant_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/value.hpp"
#include "parser/expression.hpp"

namespace duckdb {
//! Represents a constant value in the query
class ConstantExpression : public Expression {
  public:
	ConstantExpression()
	    : Expression(ExpressionType::VALUE_CONSTANT, TypeId::INTEGER), value() {
	}
	ConstantExpression(Value val)
	    : Expression(ExpressionType::VALUE_CONSTANT, val.type), value(val) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual ExpressionClass GetExpressionClass() override {
		return ExpressionClass::CONSTANT;
	}

	virtual std::unique_ptr<Expression> Copy() override;

	//! Serializes an Expression to a stand-alone binary blob
	virtual void Serialize(Serializer &serializer) override;
	//! Deserializes a blob back into an ConstantExpression
	static std::unique_ptr<Expression>
	Deserialize(ExpressionDeserializeInformation *info, Deserializer &source);

	//! Resolve the type of the constant
	virtual void ResolveType() override;

	virtual bool Equals(const Expression *other_) override;
	virtual std::string ToString() const override { return value.ToString(); }

	//! The constant value referenced
	Value value;
};
} // namespace duckdb
