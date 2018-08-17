//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/expression/subquery_expression.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/statement/select_statement.hpp"
#include "parser/tableref/tableref.hpp"
// FIXME: should not include this here!
#include "execution/physical_operator.hpp"
#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

//! Represents a subquery
class SubqueryExpression : public AbstractExpression {
  public:
	SubqueryExpression()
	    : AbstractExpression(ExpressionType::SELECT_SUBQUERY) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	std::unique_ptr<SelectStatement> subquery;
	std::unique_ptr<LogicalOperator> op;
	std::unique_ptr<BindContext> context;
	std::unique_ptr<PhysicalOperator> plan;
	bool exists = false;
	bool is_correlated = false;

	virtual std::string ToString() const override {
		std::string result = GetExprName();
		if (op) {
			result += "(" + op->ToString() + ")";
		}
		return result;
	}
};
} // namespace duckdb
