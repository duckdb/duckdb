#include "duckdb/parser/parser.hpp"

#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/parsed_data/create_table_info.hpp"
#include "duckdb/parser/statement/create_statement.hpp"
#include "duckdb/parser/statement/select_statement.hpp"
#include "duckdb/parser/statement/update_statement.hpp"
#include "duckdb/parser/query_node/select_node.hpp"
#include "duckdb/parser/tableref/expressionlistref.hpp"
#include "postgres_parser.hpp"

#include "parser/parser.hpp"

namespace duckdb {
using namespace std;

Parser::Parser() {
}

void Parser::ParseQuery(string query) {
	Transformer transformer;
	{
		PostgresParser parser;
		parser.Parse(query);

		if (!parser.success) {
			throw ParserException(Parser::FormatErrorMessage(query, parser.error_message, parser.error_location));
		}

		if (!parser.parse_tree) {
			// empty statement
			return;
		}

		// if it succeeded, we transform the Postgres parse tree into a list of
		// SQLStatements
		transformer.TransformParseTree(parser.parse_tree, statements);
		n_prepared_parameters = transformer.ParamCount();
	}
	if (statements.size() > 0) {
		auto &last_statement = statements.back();
		last_statement->stmt_length = query.size() - last_statement->stmt_location;
	}
}

string Parser::FormatErrorMessage(string query, string error_message, int error_loc) {
	error_loc--;
	if (error_loc < 0 || size_t(error_loc) >= query.size()) {
		// no location in query provided
		return error_message;
	}
	size_t error_location = size_t(error_loc);
	// count the line numbers until the error location
	size_t line_number = 1;
	for(size_t i = 0; i < error_location; i++) {
		if (query[i] == '\n' || query[i] == '\r') {
			line_number++;
		}
	}
	// scan the query backwards from the error location to find the first newline
	// we also show max 40 characters before the error location, to deal with long lines
	size_t start_pos = 0;
	for(size_t i = error_location; i > 0 && error_location - i < 40; i--) {
		if (query[i - 1] == '\n' || query[i - 1] == '\r') {
			start_pos = i;
			break;
		}
	}
	// find the end of the line, or max 40 characters AFTER the error location
	size_t end_pos = query.size() - 1;
	for(size_t i = error_location; i < query.size() - 1 && i - error_location < 40; i++) {
		if (query[i] == '\n' || query[i] == '\r') {
			end_pos = i;
			break;
		}
	}
	string line_indicator = "LINE " + to_string(line_number) + ": ";

	// now first print the error message plus the current line (or a subset of the line)
	error_message += "\n" + line_indicator + query.substr(start_pos, end_pos - start_pos);
	// print an arrow pointing at the error location
	error_message += "\n" + string(error_location - start_pos + line_indicator.size(), ' ') + "^";
	return error_message;
}

vector<unique_ptr<ParsedExpression>> Parser::ParseExpressionList(string select_list) {
	// construct a mock query prefixed with SELECT
	string mock_query = "SELECT " + select_list;
	// parse the query
	Parser parser;
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = (SelectStatement &)*parser.statements[0];
	if (select.node->type != QueryNodeType::SELECT_NODE) {
		throw ParserException("Expected a single SELECT node");
	}
	auto &select_node = (SelectNode &)*select.node;
	return move(select_node.select_list);
}

vector<OrderByNode> Parser::ParseOrderList(string select_list) {
	// construct a mock query
	string mock_query = "SELECT * FROM tbl ORDER BY " + select_list;
	// parse the query
	Parser parser;
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = (SelectStatement &)*parser.statements[0];
	if (select.node->type != QueryNodeType::SELECT_NODE) {
		throw ParserException("Expected a single SELECT node");
	}
	auto &select_node = (SelectNode &)*select.node;
	if (select_node.modifiers.size() == 0 || select_node.modifiers[0]->type != ResultModifierType::ORDER_MODIFIER) {
		throw ParserException("Expected a single ORDER clause");
	}
	auto &order = (OrderModifier &)*select_node.modifiers[0];
	return move(order.orders);
}

void Parser::ParseUpdateList(string update_list, vector<string> &update_columns,
                             vector<unique_ptr<ParsedExpression>> &expressions) {
	// construct a mock query
	string mock_query = "UPDATE tbl SET " + update_list;
	// parse the query
	Parser parser;
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::UPDATE_STATEMENT) {
		throw ParserException("Expected a single UPDATE statement");
	}
	auto &update = (UpdateStatement &)*parser.statements[0];
	update_columns = move(update.columns);
	expressions = move(update.expressions);
}

vector<vector<unique_ptr<ParsedExpression>>> Parser::ParseValuesList(string value_list) {
	// construct a mock query
	string mock_query = "VALUES " + value_list;
	// parse the query
	Parser parser;
	parser.ParseQuery(mock_query);
	// check the statements
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::SELECT_STATEMENT) {
		throw ParserException("Expected a single SELECT statement");
	}
	auto &select = (SelectStatement &)*parser.statements[0];
	if (select.node->type != QueryNodeType::SELECT_NODE) {
		throw ParserException("Expected a single SELECT node");
	}
	auto &select_node = (SelectNode &)*select.node;
	if (!select_node.from_table || select_node.from_table->type != TableReferenceType::EXPRESSION_LIST) {
		throw ParserException("Expected a single VALUES statement");
	}
	auto &values_list = (ExpressionListRef &)*select_node.from_table;
	return move(values_list.values);
}

vector<ColumnDefinition> Parser::ParseColumnList(string column_list) {
	string mock_query = "CREATE TABLE blabla (" + column_list + ")";
	Parser parser;
	parser.ParseQuery(mock_query);
	if (parser.statements.size() != 1 || parser.statements[0]->type != StatementType::CREATE_STATEMENT) {
		throw ParserException("Expected a single CREATE statement");
	}
	auto &create = (CreateStatement &)*parser.statements[0];
	if (create.info->type != CatalogType::TABLE_ENTRY) {
		throw ParserException("Expected a single CREATE TABLE statement");
	}
	auto &info = ((CreateTableInfo &)*create.info);
	return move(info.columns);
}

} // namespace duckdb
