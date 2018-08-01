//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/join_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/expression/tableref_expression.hpp"

namespace duckdb {
//! Represents a JOIN between two expressions
class JoinExpression : public TableRefExpression {
  public:
	JoinExpression() : TableRefExpression(TableReferenceType::JOIN) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }
	virtual std::string ToString() const override { return std::string(); }

	//! The left hand side of the join
	std::unique_ptr<AbstractExpression> left;
	//! The right hand side of the join
	std::unique_ptr<AbstractExpression> right;
	//! The join condition
	std::unique_ptr<AbstractExpression> condition;
	//! The join type
	JoinType type;
};
} // namespace duckdb