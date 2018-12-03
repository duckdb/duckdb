//===----------------------------------------------------------------------===// 
// 
//                         DuckDB 
// 
// planner/planner.hpp
// 
// 
// 
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "parser/sql_statement.hpp"

#include "planner/bindcontext.hpp"
#include "planner/logical_operator.hpp"

namespace duckdb {
class ClientContext;

//! The planner creates a logical query plan from the parsed SQL statements
//! using the Binder and LogicalPlanGenerator.
class Planner {
  public:
	bool CreatePlan(ClientContext &catalog,
	                std::unique_ptr<SQLStatement> statement);

	bool GetSuccess() const {
		return success;
	}
	const std::string &GetErrorMessage() const {
		return message;
	}

	bool success;
	std::string message;

	std::unique_ptr<BindContext> context;
	std::unique_ptr<LogicalOperator> plan;

  private:
	void CreatePlan(ClientContext &, SQLStatement &statement);
};
} // namespace duckdb
