//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// planner/planner.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.hpp"
#include "parser/statement/sql_statement.hpp"

#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {

//! The planner creates a logical query plan from the parsed SQL statements
//! using the Binder and LogicalPlanGenerator.
class Planner {
  public:
	bool CreatePlan(Catalog &catalog, std::unique_ptr<SQLStatement> statement);

	bool GetSuccess() const { return success; }
	const std::string &GetErrorMessage() const { return message; }

	bool success;
	std::string message;

	std::unique_ptr<BindContext> context;
	std::unique_ptr<LogicalOperator> plan;

  private:
	void CreatePlan(Catalog &, SQLStatement &statement);
};
} // namespace duckdb
