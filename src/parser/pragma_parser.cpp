#include "duckdb/parser/pragma_parser.hpp"
#include "duckdb/main/client_context.hpp"
#include "duckdb/main/database.hpp"
#include "duckdb/storage/storage_manager.hpp"
#include "duckdb/storage/buffer_manager.hpp"
#include "duckdb/common/operator/cast_operators.hpp"
#include <cctype>

using namespace duckdb;
using namespace std;

PragmaParser::PragmaParser(ClientContext &context) : context(context) {
}

bool PragmaParser::ParsePragma(string &query) {
	// check if there is a PRAGMA statement, this is done before calling the
	// postgres parser
	static const string pragma_string = "PRAGMA";
	auto query_cstr = query.c_str();

	// skip any spaces
	index_t pos = 0;
	while (isspace(query_cstr[pos])) {
		pos++;
	}

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
		new_query = "SELECT * FROM pragma_" + query.substr(keyword_start);
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
			throw ParserException("Profiling output must be an assignment");
		}
		string location = StringUtil::Replace(StringUtil::Lower(query.substr(pos + 1)), ";", "");
		context.profiler.save_location = location;
	} else if (keyword == "memory_limit") {
		if (type != PragmaType::ASSIGNMENT) {
			throw ParserException("Memory limit must be an assignment (e.g. PRAGMA memory_limit=1GB)");
		}
		ParseMemoryLimit(query.substr(pos + 1));
	} else {
		throw ParserException("Unrecognized PRAGMA keyword: %s", keyword.c_str());
	}

	return true;
}

void PragmaParser::ParseMemoryLimit(string arg) {
	// split based on the number/non-number
	index_t idx = 0;
	while (std::isspace(arg[idx])) {
		idx++;
	}
	index_t num_start = idx;
	while ((arg[idx] >= '0' && arg[idx] <= '9') || arg[idx] == '.' || arg[idx] == 'e' || arg[idx] == 'E' ||
	       arg[idx] == '-') {
		idx++;
	}
	if (idx == num_start) {
		throw ParserException("Memory limit must have a number (e.g. PRAGMA memory_limit=1GB");
	}
	string number = arg.substr(num_start, idx - num_start);

	// try to parse the number
	double limit = Cast::Operation<const char *, double>(number.c_str());

	// now parse the memory limit unit (e.g. bytes, gb, etc)
	while (std::isspace(arg[idx])) {
		idx++;
	}
	index_t start = idx;
	while (idx < arg.size() && !std::isspace(arg[idx])) {
		idx++;
	}
	if (limit < 0) {
		// limit < 0, set limit to infinite
		context.db.storage->buffer_manager->SetLimit();
		return;
	}
	string unit = StringUtil::Lower(arg.substr(start, idx - start));
	index_t multiplier;
	if (unit == "byte" || unit == "bytes" || unit == "b") {
		multiplier = 1;
	} else if (unit == "kilobyte" || unit == "kilobytes" || unit == "kb" || unit == "k") {
		multiplier = 1000LL;
	} else if (unit == "megabyte" || unit == "megabytes" || unit == "mb" || unit == "m") {
		multiplier = 1000LL * 1000LL;
	} else if (unit == "gigabyte" || unit == "gigabytes" || unit == "gb" || unit == "g") {
		multiplier = 1000LL * 1000LL * 1000LL;
	} else if (unit == "terabyte" || unit == "terabytes" || unit == "tb" || unit == "t") {
		multiplier = 1000LL * 1000LL * 1000LL * 1000LL;
	} else {
		throw ParserException("Unknown unit for memory_limit: %s (expected: b, mb, gb or tb)", unit.c_str());
	}
	// set the new limit in the buffer manager
	context.db.storage->buffer_manager->SetLimit((index_t)(multiplier * limit));
}
