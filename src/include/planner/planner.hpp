
#pragma once

#include <string>
#include <vector>

#include "catalog/catalog.hpp"
#include "parser/statement/sql_statement.hpp"

namespace duckdb {

class Planner {
  public:
	bool CreatePlan(Catalog &catalog, std::unique_ptr<SQLStatement> statement);

	bool GetSuccess() const { return success; }
	const std::string &GetErrorMessage() const { return message; }

	bool success;
	std::string message;

  private:
	void CreatePlan(Catalog &, SelectStatement &statement);
};
}
