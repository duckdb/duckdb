//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/default_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/types/value.hpp"
#include "parser/expression.hpp"

namespace duckdb {
//! Represents the default value of a column
class DefaultExpression : public Expression {
  public:
	DefaultExpression() : Expression(ExpressionType::VALUE_DEFAULT) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual ExpressionClass GetExpressionClass() override {
		return ExpressionClass::DEFAULT;
	}

	virtual std::string ToString() const override { return "Default"; }
};
} // namespace duckdb
