//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parser.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "parser/sql_statement.hpp"

#include <string>
#include <vector>

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
	void ParseQuery(std::string query);

	//! The parsed SQL statements from an invocation to ParseQuery.
	std::vector<std::unique_ptr<SQLStatement>> statements;

	private:
	//! Transform a Postgres parse tree into a set of SQL Statements
	bool TransformList(postgres::List *tree);
	//! Transform a single Postgres parse node into a SQL Statement.
	std::unique_ptr<SQLStatement> TransformNode(postgres::Node *stmt);
	//! Attempts to parse a PRAGMA statement, returns true if successfully
	//! parsed
	bool ParsePragma(std::string &query);
};
} // namespace duckdb
