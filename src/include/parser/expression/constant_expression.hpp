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

	//! Resolve the type of the constant
	virtual void ResolveType() override {
		Expression::ResolveType();
		stats = Statistics(value);
	}

	virtual bool Equals(const Expression *other_) override {
		if (!Expression::Equals(other_)) {
			return false;
		}
		auto other = reinterpret_cast<const ConstantExpression *>(other_);
		if (!other) {
			return false;
		}
		return Value::Equals(value, other->value);
	}
	virtual std::string ToString() const override { return value.ToString(); }

	//! The constant value referenced
	Value value;
};
} // namespace duckdb
