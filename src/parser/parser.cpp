#include "duckdb/parser/parser.hpp"

#include "duckdb/parser/transformer.hpp"
#include "duckdb/parser/pragma_parser.hpp"
#include "postgres_parser.hpp"

namespace postgres {
#include "parser/parser.h"
}

using namespace duckdb;
using namespace std;

Parser::Parser(ClientContext &context) : context(context) {
}

void Parser::ParseQuery(string query) {
	// first try to parse any PRAGMA statements
	PragmaParser pragma_parser(context);
	if (pragma_parser.ParsePragma(query)) {
		// query parsed as pragma statement
		// check if there is a new query to replace the current one
		if (!pragma_parser.new_query.empty()) {
			ParseQuery(pragma_parser.new_query);
		}
		return;
	}

	postgres::PostgresParser parser;
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
}
