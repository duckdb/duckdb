//===----------------------------------------------------------------------===//
//                         DuckDB
//
// parser/parser.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"

namespace postgres {
struct Node;
struct List;
} // namespace postgres

namespace duckdb {
class ClientContext;

//! The parser is responsible for parsing the query and converting it into a set
//! of parsed statements. The parsed statements can then be converted into a
//! plan and executed.
class Parser {
public:
	Parser(ClientContext &context);

	//! Attempts to parse a query into a series of SQL statements. Returns
	//! whether or not the parsing was successful. If the parsing was
	//! successful, the parsed statements will be stored in the statements
	//! variable.
	void ParseQuery(string query);

	//! The parsed SQL statements from an invocation to ParseQuery.
	vector<unique_ptr<SQLStatement>> statements;

	index_t n_prepared_parameters = 0;

private:
	ClientContext &context;
	//! Transform a Postgres parse tree into a set of SQL Statements
	bool TransformList(postgres::List *tree);
	//! Transform a single Postgres parse node into a SQL Statement.
	unique_ptr<SQLStatement> TransformNode(postgres::Node *stmt);
};
} // namespace duckdb
