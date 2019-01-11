//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/logical_plan_generator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "common/common.hpp"
#include "common/printable.hpp"
#include "parser/sql_node_visitor.hpp"
#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! The logical plan generator generates a logical query plan from a parsed SQL
//! statement
class LogicalPlanGenerator : public SQLNodeVisitor {
public:
	LogicalPlanGenerator(ClientContext &context, BindContext &bind_context)
	    : require_row_id(false), context(context), bind_context(bind_context) {
	}

	void Visit(SelectStatement &statement);
	void Visit(InsertStatement &statement);
	void Visit(CopyStatement &statement);
	void Visit(DeleteStatement &statement);
	void Visit(UpdateStatement &statement);
	void Visit(CreateTableStatement &statement);
	void Visit(CreateIndexStatement &statement);

	void VisitQueryNode(QueryNode &statement);
	void Visit(SelectNode &statement);
	void Visit(SetOperationNode &statement);

	void Visit(AggregateExpression &expr);
	void Visit(ComparisonExpression &expr);
	void Visit(CaseExpression &expr);
	void Visit(ConjunctionExpression &expr);
	void Visit(OperatorExpression &expr);
	void Visit(SubqueryExpression &expr);

	unique_ptr<TableRef> Visit(BaseTableRef &expr);
	unique_ptr<TableRef> Visit(CrossProductRef &expr);
	unique_ptr<TableRef> Visit(JoinRef &expr);
	unique_ptr<TableRef> Visit(SubqueryRef &expr);
	unique_ptr<TableRef> Visit(TableFunction &expr);

	void Print() {
		root->Print();
	}

	//! The resulting plan
	unique_ptr<LogicalOperator> root;

private:
	//! Whether or not we require row ids to be projected
	bool require_row_id = false;
	//! A reference to the catalog
	ClientContext &context;
	//! A reference to the current bind context
	BindContext &bind_context;
};
} // namespace duckdb
