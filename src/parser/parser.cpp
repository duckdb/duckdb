#include "parser/parser.hpp"

#include "main/client_context.hpp"
#include "parser/transformer.hpp"
#include "postgres_parser.hpp"

namespace postgres {
#include "parser/parser.h"
}

using namespace postgres;

using namespace duckdb;
using namespace std;

Parser::Parser(ClientContext &context) : context(context) {
}

void Parser::ParseQuery(string query) {
	// first try to parse any PRAGMA statements
	if (ParsePragma(query)) {
		// query parsed as pragma statement
		// if there was no error we were successful
		return;
	}

	PostgresParser parser;
	parser.Parse(query);

	if (!parser.success) {
		throw ParserException(parser.error_message + "[" + to_string(parser.error_location) + "]");
		return;
	}

	if (!parser.parse_tree) {
		// empty statement
		return;
	}

	// if it succeeded, we transform the Postgres parse tree into a list of
	// SQLStatements
	Transformer transformer;
	transformer.TransformParseTree(parser.parse_tree, statements);
}

enum class PragmaType : uint8_t { NOTHING, ASSIGNMENT, CALL };

bool Parser::ParsePragma(string &query) {
	// check if there is a PRAGMA statement, this is done before calling the
	// postgres parser
	static const string pragma_string = "PRAGMA";
	auto query_cstr = query.c_str();

	// skip any spaces
	index_t pos = 0;
	while (isspace(query_cstr[pos]))
		pos++;

	if (pos + pragma_string.size() >= query.size()) {
		// query is too small, can't contain PRAGMA
		return false;
	}

	if (query.compare(pos, pragma_string.size(), pragma_string.c_str()) != 0) {
		// statement does not start with PRAGMA
		return false;
	}
	pos += pragma_string.size();
	// string starts with PRAGMA, parse the pragma
	// first skip any spaces
	while (isspace(query_cstr[pos]))
		pos++;
	// now look for the keyword
	index_t keyword_start = pos;
	while (query_cstr[pos] && query_cstr[pos] != ';' && query_cstr[pos] != '=' && query_cstr[pos] != '(' &&
	       !isspace(query_cstr[pos]))
		pos++;

	// no keyword found
	if (pos == keyword_start) {
		throw ParserException("Invalid PRAGMA: PRAGMA without keyword");
	}

	string keyword = query.substr(keyword_start, pos - keyword_start);

	while (isspace(query_cstr[pos]))
		pos++;

	PragmaType type;
	if (query_cstr[pos] == '=') {
		// assignment
		type = PragmaType::ASSIGNMENT;
	} else if (query_cstr[pos] == '(') {
		// function call
		type = PragmaType::CALL;
	} else {
		// nothing
		type = PragmaType::NOTHING;
	}

	if (keyword == "table_info") {
		if (type != PragmaType::CALL) {
			throw ParserException("Invalid PRAGMA table_info: expected table name");
		}
		ParseQuery("SELECT * FROM pragma_" + query.substr(keyword_start));
	} else if (keyword == "enable_profile" || keyword == "enable_profiling") {
		// enable profiling
		if (type == PragmaType::ASSIGNMENT) {
			string assignment = StringUtil::Replace(StringUtil::Lower(query.substr(pos + 1)), ";", "");
			if (assignment == "json") {
				context.profiler.automatic_print_format = ProfilerPrintFormat::JSON;
			} else if (assignment == "query_tree") {
				context.profiler.automatic_print_format = ProfilerPrintFormat::QUERY_TREE;
			} else {
				throw ParserException("Unrecognized print format %s, supported formats: [json, query_tree]",
				                      assignment.c_str());
			}
		} else if (type == PragmaType::NOTHING) {
			context.profiler.automatic_print_format = ProfilerPrintFormat::QUERY_TREE;
		} else {
			throw ParserException("Cannot call PRAGMA enable_profiling");
		}
		context.profiler.Enable();
	} else if (keyword == "disable_profile" || keyword == "disable_profiling") {
		// enable profiling
		context.profiler.Disable();
		context.profiler.automatic_print_format = ProfilerPrintFormat::NONE;
	} else if (keyword == "profiling_output" || keyword == "profile_output") {
		// set file location of where to save profiling output
		if (type != PragmaType::ASSIGNMENT) {
			throw ParserException("Profiling output must be an assignmnet");
		}
		string location = StringUtil::Replace(StringUtil::Lower(query.substr(pos + 1)), ";", "");
		context.profiler.save_location = location;
	} else {
		throw ParserException("Unrecognized PRAGMA keyword: %s", keyword.c_str());
	}

	return true;
}
