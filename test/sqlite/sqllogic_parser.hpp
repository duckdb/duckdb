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

enum class SQLLogicTokenType {
	SQLLOGIC_INVALID,
	SQLLOGIC_SKIP_IF,
	SQLLOGIC_ONLY_IF,
	SQLLOGIC_STATEMENT,
	SQLLOGIC_QUERY,
	SQLLOGIC_HASH_THRESHOLD,
	SQLLOGIC_HALT,
	SQLLOGIC_MODE,
	SQLLOGIC_SET,
	SQLLOGIC_LOOP,
	SQLLOGIC_FOREACH,
	SQLLOGIC_CONCURRENT_LOOP,
	SQLLOGIC_CONCURRENT_FOREACH,
	SQLLOGIC_ENDLOOP,
	SQLLOGIC_REQUIRE,
	SQLLOGIC_REQUIRE_ENV,
	SQLLOGIC_LOAD,
	SQLLOGIC_RESTART,
	SQLLOGIC_RECONNECT,
	SQLLOGIC_SLEEP,
	SQLLOGIC_UNZIP
};

class SQLLogicToken {
public:
	SQLLogicTokenType type;
	vector<string> parameters;
};

class SQLLogicParser {
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
	static bool IsSingleLineStatement(SQLLogicToken &token);

	//! Does the next line contain a comment, empty line, or is the end of the file
	bool NextLineEmptyOrComment();

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
	string ExtractStatement();

	//! Extract the expected result
	vector<string> ExtractExpectedResult();

	//! Extract the expected error (in case of statement error)
	string ExtractExpectedError(bool expect_ok, bool original_sqlite_test);

	//! Tokenize the current line
	SQLLogicToken Tokenize();

	template <typename... Args>
	void Fail(const string &msg, Args... params) {
		vector<ExceptionFormatValue> values;
		FailRecursive(msg, values, params...);
	}

private:
	SQLLogicTokenType CommandToToken(const string &token);

	void FailRecursive(const string &msg, vector<ExceptionFormatValue> &values);

	template <class T, typename... Args>
	void FailRecursive(const string &msg, vector<ExceptionFormatValue> &values, T param, Args... params) {
		values.push_back(ExceptionFormatValue::CreateFormatValue<T>(param));
		FailRecursive(msg, values, params...);
	}
};

} // namespace duckdb
