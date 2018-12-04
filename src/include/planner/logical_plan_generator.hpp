//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/logical_plan_generator.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <vector>

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
	    : require_row_id(false), context(context),
	      bind_context(bind_context) {
	}

	std::unique_ptr<SQLStatement> Visit(SelectStatement &statement);
	std::unique_ptr<SQLStatement> Visit(InsertStatement &statement);
	std::unique_ptr<SQLStatement> Visit(CopyStatement &statement);
	std::unique_ptr<SQLStatement> Visit(DeleteStatement &statement);
	std::unique_ptr<SQLStatement> Visit(UpdateStatement &statement);
	std::unique_ptr<SQLStatement> Visit(CreateTableStatement &statement);
	std::unique_ptr<SQLStatement> Visit(CreateIndexStatement &statement);

	void Visit(SelectNode &statement);
    void Visit(SetOperationNode &statement);

	std::unique_ptr<Expression> Visit(AggregateExpression &expr);
	std::unique_ptr<Expression> Visit(ComparisonExpression &expr);
	std::unique_ptr<Expression> Visit(CaseExpression &expr);
	std::unique_ptr<Expression> Visit(ConjunctionExpression &expr);
	std::unique_ptr<Expression> Visit(OperatorExpression &expr);
	std::unique_ptr<Expression> Visit(SubqueryExpression &expr);

	std::unique_ptr<TableRef> Visit(BaseTableRef &expr);
	std::unique_ptr<TableRef> Visit(CrossProductRef &expr);
	std::unique_ptr<TableRef> Visit(JoinRef &expr);
	std::unique_ptr<TableRef> Visit(SubqueryRef &expr);
	std::unique_ptr<TableRef> Visit(TableFunction &expr);

	void Print() {
		root->Print();
	}

	//! The resulting plan
	std::unique_ptr<LogicalOperator> root;

  private:
	//! Whether or not we require row ids to be projected
	bool require_row_id = false;
	//! A reference to the catalog
	ClientContext &context;
	//! A reference to the current bind context
	BindContext &bind_context;
};
} // namespace duckdb
