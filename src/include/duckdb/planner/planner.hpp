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
#include "duckdb/catalog/catalog_entry/prepared_statement_catalog_entry.hpp"

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
	unordered_map<idx_t, PreparedValueEntry> value_map;

	Binder binder;
	ClientContext &context;

	bool read_only;
	bool requires_valid_transaction;

private:
	void CreatePlan(SQLStatement &statement);

	// void VerifyQuery(BoundSQLStatement &statement);
	// void VerifyNode(BoundQueryNode &statement);
	// void VerifyExpression(Expression &expr, vector<unique_ptr<Expression>> &copies);

	// bool StatementRequiresValidTransaction(BoundSQLStatement &statement);
};
} // namespace duckdb
