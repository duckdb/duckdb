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
#include "parser/expression/abstract_expression.hpp"

namespace duckdb {
//! Represents a constant value in the query
class ConstantExpression : public AbstractExpression {
  public:
	ConstantExpression()
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT, TypeId::INTEGER),
	      value() {}
	ConstantExpression(Value val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT, val.type),
	      value(val) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	//! Resolve the type of the constant
	virtual void ResolveType() override {
		AbstractExpression::ResolveType();
		stats = Statistics(value);
	}

	virtual bool Equals(const AbstractExpression *other_) override {
		if (!AbstractExpression::Equals(other_)) {
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
