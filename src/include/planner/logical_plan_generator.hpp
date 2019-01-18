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

	void CreatePlan(SQLStatement &statement);

protected:
	void CreatePlan(SelectStatement &statement);
	void CreatePlan(InsertStatement &statement);
	void CreatePlan(CopyStatement &statement);
	void CreatePlan(DeleteStatement &statement);
	void CreatePlan(UpdateStatement &statement);
	void CreatePlan(CreateTableStatement &statement);
	void CreatePlan(CreateIndexStatement &statement);

	void CreatePlan(QueryNode &statement);
	void CreatePlan(SelectNode &statement);
	void CreatePlan(SetOperationNode &statement);

	void VisitQueryNode(QueryNode &statement);

	void Visit(AggregateExpression &expr) override;
	void Visit(ComparisonExpression &expr) override;
	void Visit(CaseExpression &expr) override;
	void Visit(ConjunctionExpression &expr) override;
	void Visit(OperatorExpression &expr) override;
	unique_ptr<Expression> VisitReplace(BoundSubqueryExpression &expr, unique_ptr<Expression> *expr_ptr) override;

public:
	unique_ptr<TableRef> Visit(BaseTableRef &expr) override;
	unique_ptr<TableRef> Visit(CrossProductRef &expr) override;
	unique_ptr<TableRef> Visit(JoinRef &expr) override;
	unique_ptr<TableRef> Visit(SubqueryRef &expr) override;
	unique_ptr<TableRef> Visit(TableFunction &expr) override;

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
