//===----------------------------------------------------------------------===//
//
//                         DuckDB
//
// parser/parser.hpp
//
// Author: Mark Raasveldt
//
//===----------------------------------------------------------------------===//

#pragma once

#include <string>
#include <vector>

#include "parser/sql_statement.hpp"

namespace postgres {
struct Node;
struct List;
} // namespace postgres

namespace duckdb {

//! The parser is responsible for parsing the query and converting it into a set
//! of parsed statements. The parsed statements can then be converted into a
//! plan and executed.
class Parser {
  public:
	Parser();

	//! Attempts to parse a query into a series of SQL statements. Returns
	//! whether or not the parsing was successful. If the parsing was
	//! successful, the parsed statements will be stored in the statements
	//! variable.
	bool ParseQuery(const char *query);

	//! Returns whether or not the parsing was successful.
	bool GetSuccess() const { return success; }
	//! If the parsing was unsuccessful, returns the error message that was
	//! generated.
	const std::string &GetErrorMessage() const { return message; }

	bool success;
	std::string message;

	//! The parsed SQL statements from an invocation to ParseQuery.
	std::vector<std::unique_ptr<SQLStatement>> statements;

  private:
	//! Transform a Postgres parse tree into a set of SQL Statements
	bool TransformList(postgres::List *tree);
	//! Transform a single Postgres parse node into a SQL Statement.
	std::unique_ptr<SQLStatement> TransformNode(postgres::Node *stmt);
};
} // namespace duckdb
