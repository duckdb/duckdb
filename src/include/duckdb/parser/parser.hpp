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
#include "duckdb/parser/simplified_token.hpp"

namespace duckdb_libpgquery {
struct PGNode;
struct PGList;
} // namespace duckdb_libpgquery

namespace duckdb {

struct ParserOptions {
	bool preserve_identifier_case = true;
	idx_t max_expression_depth = 1000;
};

//! The parser is responsible for parsing the query and converting it into a set
//! of parsed statements. The parsed statements can then be converted into a
//! plan and executed.
class Parser {
public:
	Parser(ParserOptions options = ParserOptions());

	//! The parsed SQL statements from an invocation to ParseQuery.
	vector<unique_ptr<SQLStatement>> statements;

public:
	//! Attempts to parse a query into a series of SQL statements. Returns
	//! whether or not the parsing was successful. If the parsing was
	//! successful, the parsed statements will be stored in the statements
	//! variable.
	void ParseQuery(const string &query);

	//! Tokenize a query, returning the raw tokens together with their locations
	static vector<SimplifiedToken> Tokenize(const string &query);

	//! Returns true if the given text matches a keyword of the parser
	static bool IsKeyword(const string &text);
	//! Returns a list of all keywords in the parser
	static vector<ParserKeyword> KeywordList();

	//! Parses a list of expressions (i.e. the list found in a SELECT clause)
	DUCKDB_API static vector<unique_ptr<ParsedExpression>> ParseExpressionList(const string &select_list,
	                                                                           ParserOptions options = ParserOptions());
	//! Parses a list as found in an ORDER BY expression (i.e. including optional ASCENDING/DESCENDING modifiers)
	static vector<OrderByNode> ParseOrderList(const string &select_list, ParserOptions options = ParserOptions());
	//! Parses an update list (i.e. the list found in the SET clause of an UPDATE statement)
	static void ParseUpdateList(const string &update_list, vector<string> &update_columns,
	                            vector<unique_ptr<ParsedExpression>> &expressions,
	                            ParserOptions options = ParserOptions());
	//! Parses a VALUES list (i.e. the list of expressions after a VALUES clause)
	static vector<vector<unique_ptr<ParsedExpression>>> ParseValuesList(const string &value_list,
	                                                                    ParserOptions options = ParserOptions());
	//! Parses a column list (i.e. as found in a CREATE TABLE statement)
	static vector<ColumnDefinition> ParseColumnList(const string &column_list, ParserOptions options = ParserOptions());

private:
	ParserOptions options;
};
} // namespace duckdb
