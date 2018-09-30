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

enum class SubqueryType { INVALID = 0, DEFAULT = 1, EXISTS = 2, IN = 3 };

//! Represents a subquery
class SubqueryExpression : public Expression {
  public:
	SubqueryExpression()
	    : Expression(ExpressionType::SELECT_SUBQUERY),
	      type(SubqueryType::DEFAULT) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	std::unique_ptr<SelectStatement> subquery;
	std::unique_ptr<LogicalOperator> op;
	std::unique_ptr<BindContext> context;
	std::unique_ptr<PhysicalOperator> plan;
	SubqueryType type;
	bool is_correlated = false;

	virtual std::string ToString() const override {
		std::string result = GetExprName();
		if (op) {
			result += "(" + op->ToString() + ")";
		}
		return result;
	}
	virtual bool IsScalar() override { return false; }
};
} // namespace duckdb
