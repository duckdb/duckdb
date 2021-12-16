#include "test_parser.hpp"
#include "catch.hpp"

#include <fstream>

namespace duckdb {

bool TestParser::OpenFile(const string &path) {
	this->file_name = path;

	std::ifstream infile(file_name);
	if (infile.bad()) {
		return false;
	}

	string line;
	while (std::getline(infile, line)) {
		lines.push_back(StringUtil::Replace(line, "\r", ""));
	}
	return !infile.bad();
}

bool TestParser::EmptyOrComment(const string &line) {
	return line.empty() || StringUtil::StartsWith(line, "#");
}

bool TestParser::NextStatement() {
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

void TestParser::NextLine() {
	current_line++;
}

string TestParser::ExtractStatement(bool is_query) {
	string statement;

	bool first_line = true;
	while (current_line < lines.size() && !EmptyOrComment(lines[current_line])) {
		if (is_query && lines[current_line] == "----") {
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

vector<string> TestParser::ExtractExpectedResult() {
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

void TestParser::FailRecursive(const string &msg, vector<ExceptionFormatValue> &values) {
	auto error_message =
	    file_name + ":" + to_string(current_line + 1) + ": " + ExceptionFormatValue::Format(msg, values);
	FAIL(error_message.c_str());
}

TestToken TestParser::Tokenize() {
	TestToken result;
	if (current_line >= lines.size()) {
		result.type = TestTokenType::TOKEN_INVALID;
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
		result.parameters.push_back(move(argument_list[i]));
	}
	return result;
}

TestTokenType TestParser::CommandToToken(const string &token) {
	if (token == "skipif") {
		return TestTokenType::TOKEN_SKIP_IF;
	} else if (token == "onlyif") {
		return TestTokenType::TOKEN_ONLY_IF;
	} else if (token == "statement") {
		return TestTokenType::TOKEN_STATEMENT;
	} else if (token == "query") {
		return TestTokenType::TOKEN_QUERY;
	} else if (token == "hash-threshold") {
		return TestTokenType::TOKEN_HASH_THRESHOLD;
	} else if (token == "halt") {
		return TestTokenType::TOKEN_HALT;
	} else if (token == "mode") {
		return TestTokenType::TOKEN_MODE;
	} else if (token == "loop") {
		return TestTokenType::TOKEN_LOOP;
	} else if (token == "foreach") {
		return TestTokenType::TOKEN_FOREACH;
	} else if (token == "endloop") {
		return TestTokenType::TOKEN_ENDLOOP;
	} else if (token == "require") {
		return TestTokenType::TOKEN_REQUIRE;
	} else if (token == "load") {
		return TestTokenType::TOKEN_LOAD;
	} else if (token == "restart") {
		return TestTokenType::TOKEN_RESTART;
	}
	Fail("Unrecognized parameter %s", token);
	return TestTokenType::TOKEN_INVALID;
}

} // namespace duckdb
