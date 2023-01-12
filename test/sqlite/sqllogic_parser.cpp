#include "sqllogic_parser.hpp"
#include "catch.hpp"

#include <fstream>

namespace duckdb {

bool SQLLogicParser::OpenFile(const string &path) {
	this->file_name = path;

	std::ifstream infile(file_name);
	if (infile.bad() || infile.fail()) {
		return false;
	}

	string line;
	while (std::getline(infile, line)) {
		lines.push_back(StringUtil::Replace(line, "\r", ""));
	}
	return !infile.bad();
}

bool SQLLogicParser::EmptyOrComment(const string &line) {
	return line.empty() || StringUtil::StartsWith(line, "#");
}

bool SQLLogicParser::NextLineEmptyOrComment() {
	if (current_line + 1 >= lines.size()) {
		return true;
	} else {
		return EmptyOrComment(lines[current_line + 1]);
	}
}

bool SQLLogicParser::NextStatement() {
	if (seen_statement) {
		// skip the current statement
		// but only if we have already seen a statement in the file
		while (current_line < lines.size() && !EmptyOrComment(lines[current_line])) {
			current_line++;
		}
	}
	seen_statement = true;
	// now look for the first non-empty line
	while (current_line < lines.size() && EmptyOrComment(lines[current_line])) {
		current_line++;
	}
	// return whether or not we reached the end of the file
	return current_line < lines.size();
}

void SQLLogicParser::NextLine() {
	current_line++;
}

string SQLLogicParser::ExtractStatement() {
	string statement;

	bool first_line = true;
	while (current_line < lines.size() && !EmptyOrComment(lines[current_line])) {
		if (lines[current_line] == "----") {
			break;
		}
		if (!first_line) {
			statement += "\n";
		}
		statement += lines[current_line];
		first_line = false;

		current_line++;
	}

	return statement;
}

vector<string> SQLLogicParser::ExtractExpectedResult() {
	vector<string> result;
	// skip the result line (----) if we are still reading that
	if (current_line < lines.size() && lines[current_line] == "----") {
		current_line++;
	}
	// read the expected result until we encounter a new line
	while (current_line < lines.size() && !lines[current_line].empty()) {
		result.push_back(lines[current_line]);
		current_line++;
	}
	return result;
}

string SQLLogicParser::ExtractExpectedError(bool expect_ok) {
	// check if there is an expected error at all
	if (current_line >= lines.size() || lines[current_line] != "----") {
		return string();
	}
	if (expect_ok) {
		Fail("Failed to parse statement: only statement error can have an expected error message, not statement ok");
	}
	current_line++;
	string error;
	while (current_line < lines.size() && !lines[current_line].empty()) {
		if (error.empty()) {
			error = lines[current_line];
		} else {
			Fail("Failed to parse statement error: expected single line error message");
		}
		current_line++;
	}
	return error;
}

void SQLLogicParser::FailRecursive(const string &msg, vector<ExceptionFormatValue> &values) {
	auto error_message =
	    file_name + ":" + to_string(current_line + 1) + ": " + ExceptionFormatValue::Format(msg, values);
	FAIL(error_message.c_str());
}

SQLLogicToken SQLLogicParser::Tokenize() {
	SQLLogicToken result;
	if (current_line >= lines.size()) {
		result.type = SQLLogicTokenType::SQLLOGIC_INVALID;
		return result;
	}

	vector<string> argument_list;
	auto &line = lines[current_line];
	idx_t last_pos = 0;
	for (idx_t i = 0; i < line.size(); i++) {
		if (StringUtil::CharacterIsSpace(line[i])) {
			if (i == last_pos) {
				last_pos++;
			} else {
				argument_list.push_back(line.substr(last_pos, i - last_pos));
				last_pos = i + 1;
			}
		}
	}
	if (last_pos != line.size()) {
		argument_list.push_back(line.substr(last_pos, line.size() - last_pos));
	}
	if (argument_list.empty()) {
		Fail("Empty line!?");
	}
	result.type = CommandToToken(argument_list[0]);
	for (idx_t i = 1; i < argument_list.size(); i++) {
		result.parameters.push_back(std::move(argument_list[i]));
	}
	return result;
}

// Single line statements should throw a parser error if the next line is not a comment or a newline
bool SQLLogicParser::IsSingleLineStatement(SQLLogicToken &token) {
	switch (token.type) {
	case SQLLogicTokenType::SQLLOGIC_HASH_THRESHOLD:
	case SQLLogicTokenType::SQLLOGIC_HALT:
	case SQLLogicTokenType::SQLLOGIC_MODE:
	case SQLLogicTokenType::SQLLOGIC_SET:
	case SQLLogicTokenType::SQLLOGIC_LOOP:
	case SQLLogicTokenType::SQLLOGIC_FOREACH:
	case SQLLogicTokenType::SQLLOGIC_CONCURRENT_LOOP:
	case SQLLogicTokenType::SQLLOGIC_CONCURRENT_FOREACH:
	case SQLLogicTokenType::SQLLOGIC_ENDLOOP:
	case SQLLogicTokenType::SQLLOGIC_REQUIRE:
	case SQLLogicTokenType::SQLLOGIC_REQUIRE_ENV:
	case SQLLogicTokenType::SQLLOGIC_LOAD:
	case SQLLogicTokenType::SQLLOGIC_RESTART:
		return true;

	case SQLLogicTokenType::SQLLOGIC_SKIP_IF:
	case SQLLogicTokenType::SQLLOGIC_ONLY_IF:
	case SQLLogicTokenType::SQLLOGIC_INVALID:
	case SQLLogicTokenType::SQLLOGIC_STATEMENT:
	case SQLLogicTokenType::SQLLOGIC_QUERY:
		return false;

	default:
		throw std::runtime_error("Unknown SQLLogic token found!");
	}
}

SQLLogicTokenType SQLLogicParser::CommandToToken(const string &token) {
	if (token == "skipif") {
		return SQLLogicTokenType::SQLLOGIC_SKIP_IF;
	} else if (token == "onlyif") {
		return SQLLogicTokenType::SQLLOGIC_ONLY_IF;
	} else if (token == "statement") {
		return SQLLogicTokenType::SQLLOGIC_STATEMENT;
	} else if (token == "query") {
		return SQLLogicTokenType::SQLLOGIC_QUERY;
	} else if (token == "hash-threshold") {
		return SQLLogicTokenType::SQLLOGIC_HASH_THRESHOLD;
	} else if (token == "halt") {
		return SQLLogicTokenType::SQLLOGIC_HALT;
	} else if (token == "mode") {
		return SQLLogicTokenType::SQLLOGIC_MODE;
	} else if (token == "set") {
		return SQLLogicTokenType::SQLLOGIC_SET;
	} else if (token == "loop") {
		return SQLLogicTokenType::SQLLOGIC_LOOP;
	} else if (token == "concurrentloop") {
		return SQLLogicTokenType::SQLLOGIC_CONCURRENT_LOOP;
	} else if (token == "foreach") {
		return SQLLogicTokenType::SQLLOGIC_FOREACH;
	} else if (token == "concurrentforeach") {
		return SQLLogicTokenType::SQLLOGIC_CONCURRENT_FOREACH;
	} else if (token == "endloop") {
		return SQLLogicTokenType::SQLLOGIC_ENDLOOP;
	} else if (token == "require") {
		return SQLLogicTokenType::SQLLOGIC_REQUIRE;
	} else if (token == "require-env") {
		return SQLLogicTokenType::SQLLOGIC_REQUIRE_ENV;
	} else if (token == "load") {
		return SQLLogicTokenType::SQLLOGIC_LOAD;
	} else if (token == "restart") {
		return SQLLogicTokenType::SQLLOGIC_RESTART;
	}
	Fail("Unrecognized parameter %s", token);
	return SQLLogicTokenType::SQLLOGIC_INVALID;
}

} // namespace duckdb
