//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/planner/planner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/planner/binder.hpp"
#include "duckdb/planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;
class PreparedStatementData;

//! The planner creates a logical query plan from the parsed SQL statements
//! using the Binder and LogicalPlanGenerator.
class Planner {
public:
	explicit Planner(ClientContext &context);

	void CreatePlan(unique_ptr<SQLStatement> statement);

	unique_ptr<LogicalOperator> plan;
	vector<string> names;
	vector<LogicalType> types;
	unordered_map<idx_t, vector<unique_ptr<Value>>> value_map;
	vector<LogicalType> parameter_types;

	shared_ptr<Binder> binder;
	ClientContext &context;

	StatementProperties properties;

private:
	void CreatePlan(SQLStatement &statement);
	shared_ptr<PreparedStatementData> PrepareSQLStatement(unique_ptr<SQLStatement> statement);
	void PlanPrepare(unique_ptr<SQLStatement> statement);
	void PlanExecute(unique_ptr<SQLStatement> statement);
};
} // namespace duckdb
