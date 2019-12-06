//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/planner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! The planner creates a logical query plan from the parsed SQL statements
//! using the Binder and LogicalPlanGenerator.
class Planner {
public:
	Planner(ClientContext &context);

	void CreatePlan(unique_ptr<SQLStatement> statement);

	unique_ptr<LogicalOperator> plan;
	vector<string> names;
	vector<SQLType> sql_types;

	Binder binder;
	ClientContext &context;

private:
	void CreatePlan(SQLStatement &statement, vector<BoundParameterExpression *> *parameters = nullptr);

	void VerifyQuery(BoundSQLStatement &statement);
	void VerifyNode(BoundQueryNode &statement);
	void VerifyExpression(Expression &expr, vector<unique_ptr<Expression>> &copies);
};
} // namespace duckdb
