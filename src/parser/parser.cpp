#include "duckdb/parser/parser.hpp"

#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "postgres_parser.hpp"

#include "parser/parser.hpp"

using namespace duckdb;
using namespace std;

Parser::Parser() {
}

void Parser::ParseQuery(string query) {
	PostgresParser parser;
	parser.Parse(query);

	if (!parser.success) {
		throw ParserException("%s [%d]", parser.error_message.c_str(), parser.error_location);
	}

	if (!parser.parse_tree) {
		// empty statement
		return;
	}

	// if it succeeded, we transform the Postgres parse tree into a list of
	// SQLStatements
	Transformer transformer;
	transformer.TransformParseTree(parser.parse_tree, statements);
	n_prepared_parameters = transformer.prepared_statement_parameter_index;

	if (statements.size() > 0) {
		auto &last_statement = statements.back();
		last_statement->stmt_length = query.size() - last_statement->stmt_location;
	}
}

vector<unique_ptr<ParsedExpression>> Parser::ParseExpressionList(string select_list) {
	// construct a mock query prefixed with SELECT
	string mock_query = "SELECT " + select_list;
	// parse the query
	Parser parser;
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = (SelectStatement&) *parser.statements[0];
	if (select.node->type != QueryNodeType::SELECT_NODE) {
		throw ParserException("Expected a single SELECT node");
	}
	auto &select_node = (SelectNode&) *select.node;
	return move(select_node.select_list);
}

vector<OrderByNode> Parser::ParseOrderList(string select_list) {
	// construct a mock query
	string mock_query = "SELECT * FROM tbl ORDER BY " + select_list;
	// parse the query
	Parser parser;
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = (SelectStatement&) *parser.statements[0];
	if (select.node->type != QueryNodeType::SELECT_NODE) {
		throw ParserException("Expected a single SELECT node");
	}
	auto &select_node = (SelectNode&) *select.node;
	return move(select_node.orders);
}

void Parser::ParseUpdateList(string update_list, vector<string> &update_columns, vector<unique_ptr<ParsedExpression>> &expressions) {
	// construct a mock query
	string mock_query = "UPDATE tbl SET " + update_list;
	// parse the query
	Parser parser;
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::UPDATE) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &update = (UpdateStatement&) *parser.statements[0];
	update_columns = move(update.columns);
	expressions = move(update.expressions);
}