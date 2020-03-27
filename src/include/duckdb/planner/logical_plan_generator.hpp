//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/logical_plan_generator.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/bound_tokens.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! The logical plan generator generates a logical query plan from a parsed SQL
//! statement
class LogicalPlanGenerator {
public:
	LogicalPlanGenerator(Binder &binder, ClientContext &context);

	unique_ptr<LogicalOperator> CreatePlan(BoundSQLStatement &statement);

	unique_ptr<LogicalOperator> CreatePlan(BoundQueryNode &node);

	//! Whether or not subqueries should be planned already
	bool plan_subquery = true;

private:
	unique_ptr<LogicalOperator> CreatePlan(BoundSelectStatement &statement);
	unique_ptr<LogicalOperator> CreatePlan(BoundInsertStatement &statement);
	unique_ptr<LogicalOperator> CreatePlan(BoundCopyStatement &statement);
	unique_ptr<LogicalOperator> CreatePlan(BoundDeleteStatement &statement);
	unique_ptr<LogicalOperator> CreatePlan(BoundUpdateStatement &statement);
	unique_ptr<LogicalOperator> CreatePlan(BoundCreateStatement &statement);
	unique_ptr<LogicalOperator> CreatePlan(BoundExecuteStatement &statement);

	unique_ptr<LogicalOperator> CreatePlan(BoundSimpleStatement &statement);
	unique_ptr<LogicalOperator> CreatePlan(BoundExplainStatement &stmt);

	unique_ptr<LogicalOperator> CreatePlan(BoundSelectNode &node);
	unique_ptr<LogicalOperator> CreatePlan(BoundSetOperationNode &node);
	unique_ptr<LogicalOperator> CreatePlan(BoundRecursiveCTENode &node);

	unique_ptr<LogicalOperator> VisitQueryNode(BoundQueryNode &node, unique_ptr<LogicalOperator> root);

	unique_ptr<LogicalOperator> CreatePlan(BoundTableRef &ref);

	unique_ptr<LogicalOperator> CreatePlan(BoundBaseTableRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundCrossProductRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundJoinRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundSubqueryRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundTableFunction &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundEmptyTableRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundExpressionListRef &ref);
	unique_ptr<LogicalOperator> CreatePlan(BoundCTERef &ref);

	void PlanSubqueries(unique_ptr<Expression> *expr, unique_ptr<LogicalOperator> *root);

	unique_ptr<LogicalOperator> CastLogicalOperatorToTypes(vector<SQLType> &source_types, vector<SQLType> &target_types,
	                                                       unique_ptr<LogicalOperator> op);

public:
	bool has_unplanned_subqueries = false;

private:
	//! A reference to the current binder
	Binder &binder;
	//! Whether or not we require row ids to be projected
	bool require_row_id = false;
	//! A reference to the client context
	ClientContext &context;
};
} // namespace duckdb
