#include "duckdb/parser/parser.hpp"

#include "duckdb/parser/transformer.hpp"
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
