//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/logical_plan_generator.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

#include "common/internal_types.hpp"
#include "common/printable.hpp"

#include "catalog/client_context.hpp"

#include "parser/sql_node_visitor.hpp"

#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {
//! The logical plan generator generates a logical query plan from a parsed SQL
//! statement
class LogicalPlanGenerator : public SQLNodeVisitor {
  public:
	LogicalPlanGenerator(ClientContext &context, BindContext &bind_context)
	    : context(context), bind_context(bind_context) {}

	virtual void Visit(SelectStatement &statement) override;
	virtual void Visit(InsertStatement &statement) override;
	virtual void Visit(CopyStatement &statement) override;

	virtual void Visit(AggregateExpression &expr) override;
	virtual void Visit(ComparisonExpression &expr) override;
	virtual void Visit(ConjunctionExpression &expr) override;
	virtual void Visit(OperatorExpression &expr) override;
	virtual void Visit(SubqueryExpression &expr) override;

	virtual void Visit(BaseTableRef &expr) override;
	virtual void Visit(CrossProductRef &expr) override;
	virtual void Visit(JoinRef &expr) override;
	virtual void Visit(SubqueryRef &expr) override;

	void Print() { root->Print(); }

	//! The resulting plan
	std::unique_ptr<LogicalOperator> root;

  private:
	//! A reference to the catalog
	ClientContext &context;
	//! A reference to the current bind context
	BindContext &bind_context;
};
} // namespace duckdb
