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
#include "parser/abstract_expression.hpp"

namespace duckdb {
//! Represents the default value of a column
class DefaultExpression : public AbstractExpression {
  public:
	DefaultExpression() : AbstractExpression(ExpressionType::VALUE_DEFAULT) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	virtual std::string ToString() const override { return "Default"; }
};
} // namespace duckdb
