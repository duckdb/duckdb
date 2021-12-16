//===----------------------------------------------------------------------===//
//                         DuckDB
//
// test_parser.hpp
//
//
//===----------------------------------------------------------------------===//

#pragma once

#include "duckdb.hpp"
#include "duckdb/common/types.hpp"
#include "duckdb/common/exception_format_value.hpp"

namespace duckdb {

enum class TestTokenType {
	TOKEN_INVALID,
	TOKEN_SKIP_IF,
	TOKEN_ONLY_IF,
	TOKEN_STATEMENT,
	TOKEN_QUERY,
	TOKEN_HASH_THRESHOLD,
	TOKEN_HALT,
	TOKEN_MODE,
	TOKEN_LOOP,
	TOKEN_FOREACH,
	TOKEN_ENDLOOP,
	TOKEN_REQUIRE,
	TOKEN_LOAD,
	TOKEN_RESTART
};

class TestToken {
public:
	TestTokenType type;
	vector<string> parameters;
};

class TestParser {
public:
	string file_name;
	//! The lines of the current text file
	vector<string> lines;
	//! The current line number
	idx_t current_line = 0;
	//! Whether or not the input should be printed to stdout as it is executed
	bool print_input = false;
	//! Whether or not we have seen a statement
	bool seen_statement = false;

public:
	static bool EmptyOrComment(const string &line);

	//! Opens the file, returns whether or not reading was successful
	bool OpenFile(const string &path);

	//! Moves the current line to the beginning of the next statement
	//! Returns false if there is no next statement (i.e. we reached the end of the file)
	bool NextStatement();

	//! Move to the next line
	void NextLine();

	//! Extract a statement and move the current_line pointer forward
	//! if "is_query" is false, the statement stops at the next empty line
	//! if "is_query" is true, the statement stops at the next empty line or the next ----
	string ExtractStatement(bool is_query);

	//! Extract the expected result
	vector<string> ExtractExpectedResult();

	//! Tokenize the current line
	TestToken Tokenize();

	template <typename... Args>
	void Fail(const string &msg, Args... params) {
		vector<ExceptionFormatValue> values;
		FailRecursive(msg, values, params...);
	}

private:
	TestTokenType CommandToToken(const string &token);

	void FailRecursive(const string &msg, vector<ExceptionFormatValue> &values);

	template <class T, typename... Args>
	void FailRecursive(const string &msg, vector<ExceptionFormatValue> &values, T param, Args... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		FailRecursive(msg, values, params...);
	}
};

} // namespace duckdb
