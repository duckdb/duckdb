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
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT), value() {}
	ConstantExpression(std::string val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT, TypeId::VARCHAR),
	      value(val) {}
	ConstantExpression(int32_t val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT, TypeId::INTEGER),
	      value(val) {}
	ConstantExpression(double val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT, TypeId::DECIMAL),
	      value(val) {}
	ConstantExpression(const Value &val)
	    : AbstractExpression(ExpressionType::VALUE_CONSTANT, val.type),
	      value(val) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	virtual void ResolveStatistics() override { stats = Statistics(value); }

	virtual std::string ToString() const override { return value.ToString(); }

	//! The constant value referenced
	Value value;
};
} // namespace duckdb
