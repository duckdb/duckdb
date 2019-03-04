//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/planner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.hpp"
#include "planner/binder.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! The planner creates a logical query plan from the parsed SQL statements
//! using the Binder and LogicalPlanGenerator.
class Planner {
public:
	Planner(ClientContext &context);

	void CreatePlan(unique_ptr<SQLStatement> statement);

	unique_ptr<LogicalOperator> plan;

	Binder binder;
	ClientContext &context;
private:
	void CreatePlan(SQLStatement &statement, bool allow_parameter = false);
};
} // namespace duckdb
