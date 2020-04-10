//===----------------------------------------------------------------------===//
//                         DuckDB
//
// duckdb/parser/parser.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb/parser/sql_statement.hpp"
#include "duckdb/parser/parsed_expression.hpp"
#include "duckdb/parser/query_node.hpp"
#include "duckdb/parser/column_definition.hpp"

struct PGNode;
struct PGList;

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
	void ParseQuery(string query);

	//! Parses a list of expressions (i.e. the list found in a SELECT clause)
	static vector<unique_ptr<ParsedExpression>> ParseExpressionList(string select_list);
	//! Parses a list as found in an ORDER BY expression (i.e. including optional ASCENDING/DESCENDING modifiers)
	static vector<OrderByNode> ParseOrderList(string select_list);
	//! Parses an update list (i.e. the list found in the SET clause of an UPDATE statement)
	static void ParseUpdateList(string update_list, vector<string> &update_columns,
	                            vector<unique_ptr<ParsedExpression>> &expressions);
	//! Parses a VALUES list (i.e. the list of expressions after a VALUES clause)
	static vector<vector<unique_ptr<ParsedExpression>>> ParseValuesList(string value_list);
	//! Parses a column list (i.e. as found in a CREATE TABLE statement)
	static vector<ColumnDefinition> ParseColumnList(string column_list);

	//! The parsed SQL statements from an invocation to ParseQuery.
	vector<unique_ptr<SQLStatement>> statements;

	idx_t n_prepared_parameters = 0;

private:
	//! Transform a Postgres parse tree into a set of SQL Statements
	bool TransformList(PGList *tree);
	//! Transform a single Postgres parse node into a SQL Statement.
	unique_ptr<SQLStatement> TransformNode(PGNode *stmt);
};
} // namespace duckdb
