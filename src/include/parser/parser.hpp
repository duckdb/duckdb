
#pragma once

#include <string>
#include <vector>

#include "parser/statement/sql_statement.hpp"

struct Node;
struct List;

namespace duckdb {
class Parser {
  public:
	Parser();

	bool ParseQuery(const char *query);

	bool GetSuccess() const { return success; }
	const std::string &GetErrorMessage() const { return message; }

	bool success;
	std::string message;

	std::vector<std::unique_ptr<SQLStatement>> statements;

  private:
	bool ParseList(List *tree);
	std::unique_ptr<SQLStatement> ParseNode(Node *stmt);
};
} // namespace duckdb
