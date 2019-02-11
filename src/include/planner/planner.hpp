//===----------------------------------------------------------------------===//
//                         DuckDB
//
// planner/planner.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.hpp"
#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"

#include <string>
#include <vector>

namespace duckdb {
class ClientContext;

//! The planner creates a logical query plan from the parsed SQL statements
//! using the Binder and LogicalPlanGenerator.
class Planner {
public:
	void CreatePlan(ClientContext &catalog, unique_ptr<SQLStatement> statement);

	unique_ptr<LogicalOperator> plan;

private:
	void CreatePlan(ClientContext &, SQLStatement &statement);
};
} // namespace duckdb
