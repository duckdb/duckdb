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
#include "duckdb/planner/expression/bound_parameter_data.hpp"

namespace duckdb {
class ClientContext;
class PreparedStatementData;

//! The planner creates a logical query plan from the parsed SQL statements
//! using the Binder and LogicalPlanGenerator.
class Planner {
	friend class Binder;

public:
	explicit Planner(ClientContext &context);

	void CreatePlan(unique_ptr<SQLStatement> statement);

	unique_ptr<LogicalOperator> plan;
	vector<string> names;
	vector<LogicalType> types;
	bound_parameter_map_t value_map;
	vector<BoundParameterData> parameter_data;

	shared_ptr<Binder> binder;
	ClientContext &context;

	StatementProperties properties;

private:
	void CreatePlan(SQLStatement &statement);
	shared_ptr<PreparedStatementData> PrepareSQLStatement(unique_ptr<SQLStatement> statement);
};
} // namespace duckdb
