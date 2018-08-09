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

#include "parser/expression/tableref_expression.hpp"
#include "parser/statement/select_statement.hpp"
// FIXME: should not include this here!
#include "execution/physical_operator.hpp"
#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

//! Represents a subquery
class SubqueryExpression : public TableRefExpression {
  public:
	SubqueryExpression()
	    : TableRefExpression(TableReferenceType::SUBQUERY), op(nullptr) {}

	virtual void Accept(SQLNodeVisitor *v) override { v->Visit(*this); }

	std::unique_ptr<SelectStatement> subquery;
	std::unique_ptr<LogicalOperator> op;
	std::unique_ptr<BindContext> context;
	std::unique_ptr<PhysicalOperator> plan;
};
} // namespace duckdb
